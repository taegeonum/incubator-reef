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
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.driver.ShuffleTopologyManager;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessage;
import org.apache.reef.io.network.shuffle.topology.TopologyDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;

/**
 *
 */
public final class ImmutableShuffleTopologyManager implements ShuffleTopologyManager {

  private TopologyDescription topologyDescription;
  private ConfigurationSerializer confSerializer;
  private ShuffleLinkListener shuffleLinkListener;
  private ShuffleMessageHandler shuffleMessageHandler;

  @Inject
  public ImmutableShuffleTopologyManager(
      final TopologyDescription topologyDescription,
      final ConfigurationSerializer confSerializer) {
    this.topologyDescription = topologyDescription;
    this.confSerializer = confSerializer;
    this.shuffleLinkListener = new ShuffleLinkListener();
    this.shuffleMessageHandler = new ShuffleMessageHandler();
  }

  @Override
  public Class<? extends Name<String>> getTopologyName() {
    return topologyDescription.getTopologyName();
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
  public Configuration getTopologyConfigurationForTask(final String taskId) {
    return new TopologyConfigurationSerializer(taskId, topologyDescription, confSerializer)
        .getConfiguration();
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {

  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {

  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {

  }

  private final class ShuffleLinkListener implements LinkListener<Message<ShuffleMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleMessage> message) {

    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleMessage> message) {

    }
  }

  private final class ShuffleMessageHandler implements EventHandler<Message<ShuffleMessage>> {

    @Override
    public void onNext(final Message<ShuffleMessage> value) {

    }
  }
}

final class TaskEntity {

  private final Identifier taskId;

  TaskEntity(final Identifier taskId) {
    this.taskId = taskId;
  }

  Identifier getTaskId() {
    return taskId;
  }
}