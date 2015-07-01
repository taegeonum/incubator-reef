/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.io.network.shuffle.impl;

import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.shuffle.grouping.Grouping;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessage;
import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
import org.apache.reef.io.network.shuffle.utils.BroadcastEventHandler;
import org.apache.reef.io.network.shuffle.utils.BroadcastLinkListener;
import org.apache.reef.io.network.shuffle.utils.SimpleNetworkMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.*;

/**
 *
 */
public final class ImmutableShuffleTopologyClient implements ShuffleTopologyClient {

  private final Class<? extends Name<String>> topologyName;
  private final EventHandler<Message<ShuffleMessage>> shuffleMessageHandler;
  private final LinkListener<Message<ShuffleMessage>> shuffleLinkListener;
  private final Map<String, GroupingDescription> groupingDescriptionMap;
  private final Map<String, NodePoolDescription> nodePoolDescriptionMap;
  private final Map<String, NodePoolDescription> senderPoolDescriptionMap;
  private final Map<String, NodePoolDescription> receiverPoolDescriptionMap;

  private final Map<String, Codec<Tuple>> tupleCodecMap;
  private final Map<String, ShuffleTupleReceiver> receiverMap;
  private final Map<String, ShuffleTupleSender> senderMap;

  private final Map<String, BroadcastEventHandler<Message<Tuple>>> messageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<Tuple>>> linkListenerMap;

  private final ConnectionFactory<ShuffleMessage> connFactory;
  private final Injector injector;
  private final ConfigurationSerializer confSerializer;

  @Inject
  public ImmutableShuffleTopologyClient(
      final @Parameter(ShuffleTopologyName.class) String serializedTopologyName,
      final @Parameter(SerializedNodePoolSet.class) Set<String> nodePoolSet,
      final @Parameter(SerializedGroupingSet.class) Set<String> groupingSet,
      final @Parameter(SerializedSenderNodePoolSet.class) Set<String> senderNodePoolSet,
      final @Parameter(SerializedReceiverNodePoolSet.class) Set<String> receiverNodePoolSet,
      final NetworkService networkService,
      final Injector injector,
      final ConfigurationSerializer confSerializer) {

    try {
      this.topologyName = (Class<? extends Name<String>>) Class.forName(serializedTopologyName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.shuffleMessageHandler = new ShuffleMessageHandler();
    this.shuffleLinkListener = new ShuffleLinkListener();
    this.connFactory = networkService.getConnectionFactory(ShuffleNetworkServiceId.class);
    this.injector = injector;
    this.confSerializer = confSerializer;
    this.groupingDescriptionMap = new HashMap<>();
    this.nodePoolDescriptionMap = new HashMap<>();
    this.tupleCodecMap = new HashMap<>();
    this.senderPoolDescriptionMap = new HashMap<>();
    this.receiverPoolDescriptionMap = new HashMap<>();
    this.senderMap = new HashMap<>();
    this.receiverMap = new HashMap<>();

    this.messageHandlerMap = new HashMap<>();
    this.linkListenerMap = new HashMap<>();

    deserializeNodePoolSet(nodePoolDescriptionMap, nodePoolSet);
    deserializeNodePoolSet(senderPoolDescriptionMap, senderNodePoolSet);
    deserializeNodePoolSet(receiverPoolDescriptionMap, receiverNodePoolSet);
    deserializeGroupingSet(groupingDescriptionMap, groupingSet);
    createOperators();
  }

  private void deserializeNodePoolSet(final Map<String, NodePoolDescription> descriptionMap, final Set<String> nodePoolSet) {
    for (final String serializedNodePool : nodePoolSet) {
      final NodePoolDescription nodePoolDescription;
      try {
        nodePoolDescription = injector.forkInjector(confSerializer.fromString(serializedNodePool))
            .getInstance(NodePoolDescription.class);
        descriptionMap.put(nodePoolDescription.getNodePoolName(), nodePoolDescription);
      } catch (Exception e) {
        throw new RuntimeException("An error occurred when deserializing a node pool " + serializedNodePool , e);
      }
    }
  }

  private void deserializeGroupingSet(final Map<String, GroupingDescription> descriptionMap, final Set<String> groupingSet) {
    for (final String serializedGrouping : groupingSet) {
      final GroupingDescription groupingDescription;
      try {
        groupingDescription = injector.forkInjector(confSerializer.fromString(serializedGrouping))
            .getInstance(GroupingDescription.class);
        descriptionMap.put(groupingDescription.getGroupingName(), groupingDescription);
      } catch (Exception e) {
        throw new RuntimeException("An error occurred when deserializing a node pool " + serializedGrouping , e);
      }
    }
  }

  private void createOperators() {
    for (final GroupingDescription groupingDescription : groupingDescriptionMap.values()) {
      final Configuration codecConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(ShuffleKeyCodec.class, groupingDescription.getKeyCodec())
          .bindNamedParameter(ShuffleValueCodec.class, groupingDescription.getValueCodec())
          .build();

      try {
        tupleCodecMap.put(
            groupingDescription.getGroupingName(),
            Tang.Factory.getTang().newInjector(codecConfiguration)
                .getInstance(TupleCodec.class)
        );
      } catch (InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while injecting tuple codec with " + groupingDescription, e);
      }

      createOperatorsWith(groupingDescription);
    }
  }

  private void createOperatorsWith(final GroupingDescription groupingDescription) {
    if (nodePoolDescriptionMap.keySet().contains(groupingDescription.getReceiverPoolId())) {
      createReceiverWith(groupingDescription);
    }

    if (nodePoolDescriptionMap.keySet().contains(groupingDescription.getSenderPoolId())) {
      createSenderWith(groupingDescription);
    }
  }

  private void createReceiverWith(final GroupingDescription groupingDescription) {
    final Injector forkedInjector = injector.forkInjector();
    forkedInjector.bindVolatileInstance(ShuffleTopologyClient.class, this);
    forkedInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);

    try {
      receiverMap.put(groupingDescription.getGroupingName(), forkedInjector.getInstance(ShuffleTupleReceiver.class));
      messageHandlerMap.put(groupingDescription.getGroupingName(), new BroadcastEventHandler());
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while deserializing sender with " + groupingDescription, e);
    }
  }

  private void createSenderWith(final GroupingDescription groupingDescription) {
    final Configuration senderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Grouping.class, groupingDescription.getGroupingClass())
        .build();

    final Injector forkedInjector = injector.forkInjector(senderConfiguration);
    forkedInjector.bindVolatileInstance(ShuffleTopologyClient.class, this);
    forkedInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);
    forkedInjector.bindVolatileInstance(NodePoolDescription.class, receiverPoolDescriptionMap.get(groupingDescription.getReceiverPoolId()));
    forkedInjector.bindVolatileInstance(ConnectionFactory.class, connFactory);
    forkedInjector.bindVolatileParameter(ShuffleTupleCodec.class, tupleCodecMap.get(groupingDescription.getGroupingName()));

    try {
      senderMap.put(groupingDescription.getGroupingName(), forkedInjector.getInstance(ShuffleTupleSender.class));
      linkListenerMap.put(groupingDescription.getGroupingName(), new BroadcastLinkListener());
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while deserializing sender with " + groupingDescription, e);
    }
  }

  @Override
  public Class<? extends Name<String>> getTopologyName() {
    return topologyName;
  }

  @Override
  public EventHandler<Message<ShuffleMessage>> getMessageHandler() {
    return shuffleMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleMessage>> getLinkListener() {
    return shuffleLinkListener;
  }

  @Override
  public <K, V> ShuffleTupleReceiver<K, V> getReceiver(final String groupingName) {
    if (!receiverMap.containsKey(groupingName)) {
      throw new RuntimeException("This task does not have a receiver for the grouping " + groupingName);
    }

    return receiverMap.get(groupingName);
  }

  @Override
  public <K, V> ShuffleTupleSender<K, V> getSender(final String groupingName) {
    if (!senderMap.containsKey(groupingName)) {
      throw new RuntimeException("This task does not have a sender for the grouping " + groupingName);
    }

    return senderMap.get(groupingName);
  }

  @Override
  public <K, V> void registerLinkListener(final String groupingName, final LinkListener<Message<Tuple<K, V>>> linkListener) {
    linkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  @Override
  public <K, V> void registerMessageHandler(final String groupingName, final EventHandler<Message<Tuple<K, V>>> messageHandler) {
    messageHandlerMap.get(groupingName).addEventHandler(messageHandler);
  }

  private Message<Tuple> convertShuffleMessageToTupleMessage(
      final ShuffleMessage message, final Identifier srcId, final Identifier destId) {

    final int dataLength = message.getDataLength();
    final Codec<Tuple> tupleCodec = tupleCodecMap.get(message.getGroupingName());
    final List<Tuple> tupleList = new ArrayList<>(dataLength);
    for (int i = 0; i < dataLength; i++) {
      tupleList.add(tupleCodec.decode(message.getDataAt(i)));
    }
    return new SimpleNetworkMessage<>(
        srcId,
        destId,
        tupleList
    );
  }

  private final class ShuffleMessageHandler implements EventHandler<Message<ShuffleMessage>> {

    @Override
    public void onNext(final Message<ShuffleMessage> message) {
      final ShuffleMessage shuffleMessage = message.getData().iterator().next();
      if (shuffleMessage.getCode() == ShuffleMessage.TUPLE) {
        messageHandlerMap.get(shuffleMessage.getGroupingName())
            .onNext(convertShuffleMessageToTupleMessage(shuffleMessage, message.getSrcId(), message.getDestId()));
      } else {
        //
      }
    }
  }

  private final class ShuffleLinkListener implements LinkListener<Message<ShuffleMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleMessage> message) {
      final ShuffleMessage shuffleMessage = message.getData().iterator().next();
      if (shuffleMessage.getCode() == ShuffleMessage.TUPLE) {
        linkListenerMap.get(shuffleMessage.getGroupingName())
            .onSuccess(convertShuffleMessageToTupleMessage(shuffleMessage, message.getSrcId(), message.getDestId()));
      } else {
        //
      }
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleMessage> message) {
      final ShuffleMessage shuffleMessage = message.getData().iterator().next();
      if (shuffleMessage.getCode() == ShuffleMessage.TUPLE) {
        linkListenerMap.get(shuffleMessage.getGroupingName())
            .onException(
                cause,
                remoteAddress,
                convertShuffleMessageToTupleMessage(shuffleMessage, message.getSrcId(), message.getDestId())
            );
      } else {
        //
      }
    }
  }
}
