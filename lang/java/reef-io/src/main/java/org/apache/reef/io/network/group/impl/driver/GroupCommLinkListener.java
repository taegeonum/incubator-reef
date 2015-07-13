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
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GroupComm LinkListener.
 */
public final class GroupCommLinkListener implements LinkListener<Message<GroupCommunicationMessage>> {

  private static final Logger LOG = Logger.getLogger(GroupCommLinkListener.class.getName());
  final List<Throwable> exceptions = new ArrayList<>();

  @Inject
  public GroupCommLinkListener() {

  }

  @Override
  public void onSuccess(final Message<GroupCommunicationMessage> message) {
    LOG.log(Level.FINE, "Success: {0}", message);
  }

  @Override
  public void onException(Throwable cause, SocketAddress remoteAddress, Message<GroupCommunicationMessage> message) {
    LOG.entering("ExceptionHandler", "onNext", new Object[]{cause});
    exceptions.add(cause);
    LOG.finest("Got an exception. Added it to list(" + exceptions.size() + ")");
    LOG.exiting("ExceptionHandler", "onNext");
  }
}
