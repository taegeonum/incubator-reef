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
import org.apache.reef.io.network.temp.ConnectionPool;
import org.apache.reef.io.network.temp.NetworkEvent;
import org.apache.reef.io.network.temp.NetworkService;
import org.apache.reef.io.network.temp.NetworkServiceParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by kgw on 2015. 5. 31..
 */
public final class DefaultNetworkServiceImplementation implements NetworkService {

  private final Transport transport;
  private final EventHandler<TransportEvent> recvHandler;
  private final ConcurrentMap<Identifier, NSConnectionPool> connectionPoolMap;

  @Inject
  public DefaultNetworkServiceImplementation(
      final @Parameter(NetworkServiceParameter.Port.class) int nsPort,
      final @Parameter(NetworkServiceParameter.IdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(NetworkServiceParameter.ExceptionHandler.class) EventHandler<Exception> exceptionHandler,
      final TransportFactory transportFactory) {
    connectionPoolMap = new ConcurrentHashMap<>();
    recvHandler = new NetworkServiceReceiveHandler(connectionPoolMap, idFactory, new NetworkServiceEventCodec());
    transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler, exceptionHandler);

  }

  @Override
  public void registerId(final String networkServiceId) throws NetworkException{

  }

  @Override
  public void unregisterId() throws NetworkException {

  }

  @Override
  public <T> ConnectionPool<T> newConnectionPool(final Identifier connectionId, final Codec<T> codec,
                                              final EventHandler<NetworkEvent<T>> eventHandler) {
    return newConnectionPool(connectionId, codec, eventHandler, null);
  }

  @Override
  public <T> ConnectionPool<T> newConnectionPool(final Identifier connectionId, final Codec<T> codec,
                                              final EventHandler<NetworkEvent<T>> eventHandler,
                                              final LinkListener<List<T>> linkListener) {

    return new NSConnectionPool<T>();
  }

  @Override
  public Identifier getNetworkServiceId() {
    return null;
  }

  @Override
  public void close() throws Exception {

  }

  Transport getTransport() {
    return transport;
  }

  void removeConnectionPool(Identifier connectionId) {

  }
}
