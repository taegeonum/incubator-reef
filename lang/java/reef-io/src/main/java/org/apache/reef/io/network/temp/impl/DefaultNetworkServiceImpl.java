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
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.io.network.temp.NetworkService;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
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
  private final ConcurrentMap<Identifier, NSConnectionFactory> connectionFactoryMap;
  private Identifier myId;
  private final Codec<NetworkEvent> nsCodec;
  private final LinkListener<NetworkEvent> nsLinkListener;

  @Inject
  public DefaultNetworkServiceImpl(
      final @Parameter(NetworkServiceParameters.IdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(NetworkServiceParameters.Port.class) int nsPort,
      final TransportFactory transportFactory,
      final NameClientProxy nameClientProxy) throws NetworkException {

    this.idFactory = idFactory;
    this.connectionFactoryMap = new ConcurrentHashMap<>();
    this.nsCodec = new NetworkEventCodec(idFactory, connectionFactoryMap);
    this.nsLinkListener = new NetworkServiceLinkListener(connectionFactoryMap);
    this.recvHandler = new NetworkServiceReceiveHandler(connectionFactoryMap, nsCodec);
    this.nameClientProxy = nameClientProxy;
    this.transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler, new DefaultNSExceptionHandler());
  }

  @Override
  public void registerId(final Identifier nsId) throws NetworkException {
    this.myId = nsId;
    nameClientProxy.registerId(
        nsId,
        (InetSocketAddress) transport.getLocalAddress()
    );
  }

  <T> Link<NetworkEvent<T>> openLink(Identifier remoteId) throws NetworkException {
    try {
      final SocketAddress address = nameClientProxy.lookup(remoteId);
      return transport.open(address, nsCodec, nsLinkListener);
    } catch(Exception e) {
      throw new NetworkException(e);
    }
  }

  @Override
  public <T> ConnectionFactory<T> newConnectionFactory(final Identifier clientServiceId, final Codec<T> codec,
                                                       final EventHandler<NetworkEvent<T>> eventHandler) {
    return newConnectionFactory(clientServiceId, codec, eventHandler, null);
  }

  @Override
  public <T> ConnectionFactory<T> newConnectionFactory(final Identifier clientServiceId, final Codec<T> codec,
                                              final EventHandler<NetworkEvent<T>> eventHandler,
                                              final LinkListener<NetworkEvent<T>> linkListener) {

    final NSConnectionFactory<T> connectionFactory = new NSConnectionFactory<>(this, clientServiceId,
        codec, eventHandler, linkListener);
    if (connectionFactoryMap.putIfAbsent(clientServiceId, connectionFactory) != null) {
      throw new NetworkRuntimeException(connectionFactory.toString() + " was already registered.");
    }

    return connectionFactory;
  }

  @Override
  public Identifier getNetworkServiceId() {
    return this.myId;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return transport.getLocalAddress();
  }

  @Override
  public void close() throws Exception {
    nameClientProxy.unregisterId(this.myId);
    nameClientProxy.close();
    transport.close();
  }
}
