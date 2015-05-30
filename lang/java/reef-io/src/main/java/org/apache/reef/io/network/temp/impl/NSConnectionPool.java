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

import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.io.network.temp.Connection;
import org.apache.reef.io.network.temp.ConnectionPool;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
 */

final class NSConnectionPool<T> implements ConnectionPool<T> {

  private final ConcurrentMap<Identifier, Connection<T>> connectionMap;
  private final Identifier connectionId;
  private final Codec<T> eventCodec;
  private final EventHandler<NetworkEvent<T>> eventHandler;
  private final LinkListener<NetworkEvent<T>> eventListener;
  private final NetworkServiceImpl networkServiceImpl;
  private final AtomicBoolean closed;

  public NSConnectionPool(
      final Identifier connectionId,
      final Codec<T> eventCodec,
      final EventHandler<NetworkEvent<T>> eventHandler,
      final LinkListener<NetworkEvent<T>> eventListener,
      final NetworkServiceImpl networkServiceImpl) {

    this.connectionMap = new ConcurrentHashMap<>();
    this.closed = new AtomicBoolean();
    this.connectionId = connectionId;
    this.eventCodec = eventCodec;
    this.eventHandler = eventHandler;
    this.eventListener = eventListener;
    this.networkServiceImpl = networkServiceImpl;
  }

  @Override
  public Identifier getConnectionId() {
    return connectionId;
  }

  @Override
  public Connection<T> newConnection(Identifier remoteId) {
    if (closed.get()) {
      throw new NetworkRuntimeException("newConnection(\'" + remoteId + "\') is requested but" +
          this.toString() + " was already closed");
    }

    final Connection<T> connection = new NSConnection<>(remoteId, this, networkServiceImpl);
    if (connectionMap.putIfAbsent(remoteId, connection) != null) {
      throw new NetworkRuntimeException("newConnection(\'" + remoteId + "\') is requested for " + this.toString() +
          "but Connection[" + remoteId + "] was already registered");
    }
    return connection;
  }

  @Override
  public Connection<T> getConnection(Identifier remoteId) {
    return connectionMap.get(remoteId);
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      networkServiceImpl.removeConnectionPool(connectionId);
    }
  }

  public String toString() {
    return "ConnectionPool[" + connectionId + "]";
  }

  void removeConnection(Identifier remoteId) {
    connectionMap.remove(remoteId);
  }

  EventHandler<NetworkEvent<T>> getEventHandler() {
    return eventHandler;
  }

  LinkListener<NetworkEvent<T>> getLinkListener() {
    return eventListener;
  }

  Codec<T> getCodec() {
    return eventCodec;
  }
}
