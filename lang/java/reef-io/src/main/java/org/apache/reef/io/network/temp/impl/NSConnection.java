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
package org.apache.reef.io.network.temp.impl;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.transport.Link;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 *
 */
final class NSConnection<T> implements Connection<T> {

  private Link<NetworkEvent<T>> link;

  private final Identifier remoteId;
  private final AtomicBoolean closed;
  private final NSConnectionFactory connFactory;

  public NSConnection(
      final NSConnectionFactory connFactory,
      final Identifier remoteId) {

    this.connFactory = connFactory;
    this.remoteId = remoteId;
    this.closed = new AtomicBoolean();
  }

  @Override
  public void open() throws NetworkException {
    link = connFactory.openLink(remoteId);
  }

  @Override
  public void write(List<T> messageList) {
    final NetworkEvent<T> nsServiceEvent = new NetworkEvent<>(
        connFactory.getClientServiceId(),
        connFactory.getSrcId(),
        remoteId,
        messageList);

    checkIsLinkOpened("write messages");
    link.write(nsServiceEvent);
  }

  @Override
  public void write(T message) {
    final List<T> messageList = new ArrayList<>(1);
    messageList.add(message);
    write(messageList);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      link = null;
      connFactory.removeConnection(remoteId);
    }
  }

  @Override
  public String toString() {
    return "Connection from" + connFactory.getSrcId() + ":" + connFactory.getClientServiceId() + " to " +  remoteId + ":" + connFactory.getClientServiceId()
        ;
  }

  private void checkIsLinkOpened(String request) {
    if (link == null) {
      close();
      throw new NetworkRuntimeException(this.toString() + " is not opened but " + request + " is requested");
    }
  }
}
