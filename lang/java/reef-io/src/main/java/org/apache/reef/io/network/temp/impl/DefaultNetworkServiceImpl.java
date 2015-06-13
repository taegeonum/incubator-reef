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
import org.apache.reef.io.network.temp.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default NetworkService implementation
 */
public final class DefaultNetworkServiceImpl implements NetworkService {

  private final IdentifierFactory idFactory;
  private final NameClientProxy nameClientProxy;
  private final Transport transport;
  private final EventHandler<TransportEvent> recvHandler;
  private final LinkListener<NetworkEvent> nsLinkListener;
  private final Codec<NetworkEvent> nsEventCodec;
  private final ConcurrentMap<Identifier, NSConnectionPool> connectionPoolMap;

  @Inject
  public DefaultNetworkServiceImpl(
      final @Parameter(NetworkServiceParameter.IdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(NetworkServiceParameter.Port.class) int nsPort,
      final @Parameter(NetworkServiceParameter.ExceptionHandler.class) EventHandler<Exception> exceptionHandler,
      final TransportFactory transportFactory,
      final NameClientProxy nameClientProxy) {

    this.idFactory = idFactory;
    this.connectionPoolMap = new ConcurrentHashMap<>();
    this.nsEventCodec = new NetworkEventCodec(idFactory, connectionPoolMap);
    this.nsLinkListener = new NetworkServiceLinkListener(connectionPoolMap);
    this.recvHandler = new NetworkServiceReceiveHandler(connectionPoolMap, nsEventCodec);
    this.nameClientProxy = nameClientProxy;
    this.transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler, exceptionHandler);
  }

  @Override
  public void registerId(final String networkServiceId) throws NetworkException {
    nameClientProxy.registerId(
        idFactory.getNewInstance(networkServiceId),
        (InetSocketAddress) transport.getLocalAddress()
    );
  }

  @Override
  public void unregisterId() throws NetworkException {
    nameClientProxy.unregisterId();
  }

  @Override
  public <T> ConnectionPool<T> newConnectionPool(final Identifier connectionId, final Codec<T> codec,
                                              final EventHandler<NetworkEvent<T>> eventHandler) {
    return newConnectionPool(connectionId, codec, eventHandler, null);
  }

  @Override
  public <T> ConnectionPool<T> newConnectionPool(final Identifier connectionId, final Codec<T> codec,
                                              final EventHandler<NetworkEvent<T>> eventHandler,
                                              final LinkListener<NetworkEvent<T>> linkListener) {

    final NSConnectionPool<T> connectionPool = new NSConnectionPool<>(connectionId, codec, eventHandler, linkListener, this);
    if (connectionPoolMap.putIfAbsent(connectionId, connectionPool) != null) {
      throw new NetworkRuntimeException(connectionPool.toString() + " was already registered.");
    }

    return connectionPool;
  }

  @Override
  public Identifier getNetworkServiceId() {
    return nameClientProxy.getLocalIdentifier();
  }

  @Override
  public SocketAddress getLocalAddress() {
    return transport.getLocalAddress();
  }

  @Override
  public void close() throws Exception {
    nameClientProxy.close();
    transport.close();
  }

  <T> Link<NetworkEvent<T>> openLink(Identifier remoteId) throws NetworkException {
    try {
      final SocketAddress address = nameClientProxy.lookup(remoteId);
      return transport.open(address, nsEventCodec, nsLinkListener);
    } catch(Exception e) {
      throw new NetworkException(e);
    }
  }

  void removeConnectionPool(Identifier connectionId) {
    connectionPoolMap.remove(connectionId);
  }
}
