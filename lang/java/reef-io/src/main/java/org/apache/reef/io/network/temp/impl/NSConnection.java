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
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.io.network.temp.Connection;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.transport.Link;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
final class NSConnection<T> implements Connection<T> {

  private Link<NetworkEvent<T>> link;

  private final Identifier remoteId;
  private final NSConnectionPool<T> connectionPool;
  private final NetworkServiceImpl networkServiceImpl;
  private final AtomicBoolean closed;

  public NSConnection(
      final Identifier remoteId,
      final NSConnectionPool<T> connectionPool,
      final NetworkServiceImpl networkServiceImpl) {

    this.remoteId = remoteId;
    this.connectionPool = connectionPool;
    this.closed = new AtomicBoolean();
    this.networkServiceImpl = networkServiceImpl;
  }

  @Override
  public void open() throws NetworkException {
    try {
      link = networkServiceImpl.openLink(remoteId);
    } catch (NetworkException e) {
      close();
      throw e;
    }
  }

  @Override
  public void write(List<T> messageList) {
    final NetworkEvent<T> nsServiceEvent = new NetworkEvent<>(
        connectionPool.getConnectionId(),
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
  public SocketAddress getLocalAddress() {
    checkIsLinkOpened("get local address");
    return link.getLocalAddress();
  }

  @Override
  public SocketAddress getRemoteAddress() {

    checkIsLinkOpened("get remote address");
    return link.getRemoteAddress();
  }

  @Override
  public Identifier getRemoteId() {
    return remoteId;
  }

  @Override
  public Identifier getConnectionId() {
    return connectionPool.getConnectionId();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      link = null;
      connectionPool.removeConnection(remoteId);
    }
  }

  public String toString() {
    return "Connection[" + remoteId + "] in " + connectionPool.toString();
  }

  private void checkIsLinkOpened(String request) {
    if (link == null) {
      close();
      throw new NetworkRuntimeException(this.toString() + " is not opened but " + request + " is requested");
    }
  }
}
