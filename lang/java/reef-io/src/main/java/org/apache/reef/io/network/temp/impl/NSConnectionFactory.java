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
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NetworkService connection factory
 * ClientService uses connection factory to create connections.
 */

final class NSConnectionFactory<T> implements ConnectionFactory<T> {

  private final ConcurrentMap<Identifier, Connection<T>> connectionMap;
  private final String clientName;
  private final Codec<T> eventCodec;
  private final EventHandler<NetworkEvent<T>> eventHandler;
  private final LinkListener<NetworkEvent<T>> eventListener;
  private final AtomicBoolean closed;
  private final DefaultNetworkServiceImpl networkService;


  NSConnectionFactory(
      final DefaultNetworkServiceImpl networkService,
      final String clientName,
      final Codec<T> eventCodec,
      final EventHandler<NetworkEvent<T>> eventHandler,
      final LinkListener<NetworkEvent<T>> eventListener) {

    this.networkService = networkService;
    this.connectionMap = new ConcurrentHashMap<>();
    this.closed = new AtomicBoolean();
    this.clientName = clientName;
    this.eventCodec = eventCodec;
    this.eventHandler = eventHandler;
    this.eventListener = eventListener;
  }

  @Override
  public Connection<T> newConnection(Identifier remoteNetworkServiceId) {
    if (closed.get()) {
      throw new NetworkRuntimeException("newConnection(\'" + remoteNetworkServiceId + "\') is requested but" +
          this.toString() + " was already closed");
    }

    final Connection<T> connection = connectionMap.get(remoteNetworkServiceId);

    if (connection == null) {
      final Connection<T> newConnection = new NSConnection<>(this, remoteNetworkServiceId);
      if (connectionMap.putIfAbsent(remoteNetworkServiceId, newConnection) != null) {
        return connectionMap.get(remoteNetworkServiceId);
      } else {
        return newConnection;
      }
    }

    return connection;
  }

  <T> Link<NetworkEvent<T>> openLink(Identifier remoteId) throws NetworkException {
    return networkService.openLink(remoteId);
  }

  void removeConnection(Identifier remoteId) {
    connectionMap.remove(remoteId);
  }

  String getClientServiceId() { return this.clientName; }
  Identifier getSrcId() { return this.networkService.getNetworkServiceId(); }

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
