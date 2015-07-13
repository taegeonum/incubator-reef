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

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.driver.ShuffleManager;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.params.ShuffleControlMessageNSId;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.descriptor.GroupingDescriptor;
import org.apache.reef.io.network.shuffle.descriptor.ShuffleDescriptor;
import org.apache.reef.io.network.shuffle.descriptor.ShuffleDescriptorSerializer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class StaticShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(StaticShuffleManager.class.getName());

  private final ShuffleDescriptor initialShuffleDescriptor;
  private final ShuffleDescriptorSerializer shuffleDescriptorSerializer;
  private final ShuffleLinkListener shuffleLinkListener;
  private final ShuffleMessageHandler shuffleMessageHandler;
  private final Map<String, GroupingSetupGate> groupingSetupGates;

  @Inject
  public StaticShuffleManager(
      final ShuffleDescriptor initialShuffleDescriptor,
      final ShuffleDescriptorSerializer shuffleDescriptorSerializer,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final NetworkServiceClient nsClient) {
    this.initialShuffleDescriptor = initialShuffleDescriptor;
    this.shuffleDescriptorSerializer = shuffleDescriptorSerializer;
    this.shuffleLinkListener = new ShuffleLinkListener();
    this.shuffleMessageHandler = new ShuffleMessageHandler();

    this.groupingSetupGates = new ConcurrentHashMap<>();
    createGroupingSetupGates(idFactory,
        nsClient.<ShuffleControlMessage>getConnectionFactory(ShuffleControlMessageNSId.class));
  }

  private void createGroupingSetupGates(
      final IdentifierFactory idFactory, final ConnectionFactory<ShuffleControlMessage> connFactory) {
    for (final String groupingName : initialShuffleDescriptor.getGroupingNameList()) {
      final GroupingDescriptor descriptor = initialShuffleDescriptor.getGroupingDescriptor(groupingName);
      final Set<String> taskIdSet = new HashSet<>();
      for (final String senderId : initialShuffleDescriptor.getSenderIdList(descriptor.getGroupingName())) {
        taskIdSet.add(senderId);
      }

      for (final String receiverId : initialShuffleDescriptor.getReceiverIdList(descriptor.getGroupingName())) {
        taskIdSet.add(receiverId);
      }

      groupingSetupGates.put(
          groupingName,
          new GroupingSetupGate(
              initialShuffleDescriptor.getShuffleName().getName(),
              groupingName,
              taskIdSet,
              idFactory,
              connFactory
          )
      );
    }
  }

  @Override
  public EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler() {
    return shuffleMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleControlMessage>> getControlLinkListener() {
    return shuffleLinkListener;
  }

  @Override
  public ShuffleDescriptor getShuffleDescriptor() {
    return initialShuffleDescriptor;
  }

  @Override
  public Configuration getShuffleDescriptorConfigurationForTask(final String taskId) {
    return shuffleDescriptorSerializer.getConfigurationHasTaskId(initialShuffleDescriptor, taskId);
  }

  @Override
  public Class<? extends ShuffleClient> getClientClass() {
    return StaticShuffleClient.class;
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {
    for (final GroupingSetupGate gate : groupingSetupGates.values()) {
      gate.onTaskStarted(runningTask.getId());
    }
  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {
    onTaskStopped(failedTask.getId());
  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {
    onTaskStopped(completedTask.getId());
  }

  private void onTaskStopped(final String taskId) {
    for (final GroupingSetupGate gate : groupingSetupGates.values()) {
      gate.onTaskStopped(taskId);
    }
  }

  private final class ShuffleLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "A ShuffleMessage was successfully sent : {0}", message);
    }

    @Override
    public void onException(
        final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "An exception occurred with a ShuffleMessage [{0}] caused by ",
          new Object[]{ message, cause });
    }
  }

  private final class ShuffleMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "A ShuffleMessage was arrived {0}", message);
    }
  }
}
