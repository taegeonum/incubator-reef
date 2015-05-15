/*
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
package org.apache.reef.io.network.group.impl.driver;


import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;

import javax.inject.Inject;

public final class GroupCommNSMessageHandler implements EventHandler<Message<GroupCommunicationMessage>> {

  private final SingleThreadStage<GroupCommunicationMessage> groupCommMessageStage;

  @Inject
  public GroupCommNSMessageHandler(final GroupCommMessageHandler groupCommMessageHandler) {
    this.groupCommMessageStage = new SingleThreadStage<>("GroupCommMessageStage", groupCommMessageHandler, 100 * 1000);

  }

  @Override
  public void onNext(Message<GroupCommunicationMessage> msg) {
    groupCommMessageStage.onNext(Utils.getGCM(msg));
  }
}
