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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.io.network.shuffle.ns.ShuffleLinkListener;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessageHandler;
import org.apache.reef.io.network.shuffle.params.SerializedTopologySet;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class ShuffleServiceImpl implements ShuffleService {

  private final Map<Class<? extends Name>, ShuffleTopologyClient> clientMap;

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;

  private final ShuffleMessageHandler shuffleMessageHandler;
  private final ShuffleLinkListener shuffleLinkListener;

  @Inject
  public ShuffleServiceImpl(
      final @Parameter(SerializedTopologySet.class) Set<String> serializedTopologySet,
      final Injector rootInjector,
      final ConfigurationSerializer confSerializer,
      final ShuffleMessageHandler shuffleMessageHandler,
      final ShuffleLinkListener shuffleLinkListener) {
    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.clientMap = new HashMap<>();
    this.shuffleLinkListener = shuffleLinkListener;
    this.shuffleMessageHandler = shuffleMessageHandler;
    deserializeClients(serializedTopologySet);
  }

  private void deserializeClients(final Set<String> serializedTopologySet) {
    for (final String serializedTopology : serializedTopologySet) {
      deserializeClient(serializedTopology);
    }
  }

  private void deserializeClient(final String serializedTopology) {
    try {
      final Configuration topologyConfig = confSerializer.fromString(serializedTopology);
      final Injector injector = rootInjector.forkInjector(topologyConfig);
      final ShuffleTopologyClient client = injector.getInstance(ShuffleTopologyClient.class);
      shuffleLinkListener.registerLinkListener(client.getTopologyName(), client.getLinkListener());
      shuffleMessageHandler.registerMessageHandler(client.getTopologyName(), client.getMessageHandler());
      clientMap.put(client.getTopologyName(), client);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing topology " + serializedTopology, exception);
    }
  }

  @Override
  public ShuffleTopologyClient getTopologyClient(final Class<? extends Name<String>> topologyName) {
    return clientMap.get(topologyName);
  }
}
