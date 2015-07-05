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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.Tuple;
import org.apache.reef.io.naming.Naming;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.naming.NameClient;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryCount;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryTimeout;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.*;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network service for Task.
 */
public final class NetworkService<T> implements Stage, ConnectionFactory<T> {

  private static final Logger LOG = Logger.getLogger(NetworkService.class.getName());

  private static final int RETRY_COUNT;
  private static final int RETRY_TIMEOUT;

  static {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      RETRY_COUNT = injector.getNamedInstance(NameResolverRetryCount.class);
      RETRY_TIMEOUT = injector.getNamedInstance(NameResolverRetryTimeout.class);
    } catch (final InjectionException ex) {
      final String msg = "Exception while trying to find default values for retryCount & Timeout";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  private final IdentifierFactory factory;
  private final Codec<T> codec;
  private final Transport transport;
  private final NameResolver nameResolver;
  private final ConcurrentMap<Identifier, Connection<T>> idToConnMap = new ConcurrentHashMap<>();
  private final EStage<Tuple<Identifier, InetSocketAddress>> nameServiceRegisteringStage;
  private final EStage<Identifier> nameServiceUnregisteringStage;
  private Identifier myId;

  /**
   * @param factory
   * @param nsPort
   * @param nameServerAddr
   * @param nameServerPort
   * @param codec
   * @param tpFactory
   * @param recvHandler
   * @param exHandler
   * @deprecated have an instance injected instead.
   */
  @Deprecated
  public NetworkService(final IdentifierFactory factory,
                        final int nsPort,
                        final String nameServerAddr,
                        final int nameServerPort,
                        final Codec<T> codec,
                        final TransportFactory tpFactory,
                        final EventHandler<Message<T>> recvHandler,
                        final EventHandler<Exception> exHandler,
                        final LocalAddressProvider localAddressProvider) {
    this(factory, nsPort, nameServerAddr, nameServerPort,
            RETRY_COUNT, RETRY_TIMEOUT, codec, tpFactory, recvHandler, exHandler, localAddressProvider);
  }

  /**
   * @deprecated have an instance injected instead.
   */
  @Deprecated
  public NetworkService(final IdentifierFactory factory,
                        final int nsPort,
                        final String nameServerAddr,
                        final int nameServerPort,
                        final Codec<T> codec,
                        final TransportFactory tpFactory,
                        final EventHandler<Message<T>> recvHandler,
                        final EventHandler<Exception> exHandler) {
    this(factory, nsPort, nameServerAddr, nameServerPort,
            RETRY_COUNT, RETRY_TIMEOUT, codec, tpFactory, recvHandler, exHandler, LocalAddressProviderFactory.getInstance());
  }

  /**
   * @deprecated have an instance injected instead.
   */
  @Deprecated
  public NetworkService(
      final IdentifierFactory factory,
      final int nsPort,
      final String nameServerAddr,
      final int nameServerPort,
      final int retryCount,
      final int retryTimeout,
      final Codec<T> codec,
      final TransportFactory tpFactory,
      final EventHandler<Message<T>> recvHandler,
      final EventHandler<Exception> exHandler) {
    this(factory, nsPort, nameServerAddr, nameServerPort, retryCount, retryTimeout, codec, tpFactory, recvHandler, exHandler,
        LocalAddressProviderFactory.getInstance());
  }

  /**
   * @deprecated have an instance injected instead.
   */
  @Deprecated
  public NetworkService(
      final IdentifierFactory factory,
      final int nsPort,
      final String nameServerAddr,
      final int nameServerPort,
      final int retryCount,
      final int retryTimeout,
      final Codec<T> codec,
      final TransportFactory tpFactory,
      final EventHandler<Message<T>> recvHandler,
      final EventHandler<Exception> exHandler,
      final LocalAddressProvider localAddressProvider) {
    this(factory, nsPort, new NameClient(nameServerAddr, nameServerPort,
        30000, factory, retryCount, retryTimeout, localAddressProvider, tpFactory),
        codec, tpFactory, recvHandler, exHandler, localAddressProvider);
  }

  /**
   * @deprecated in 0.12. Use Tang to obtain an instance of this instead.
   */
  @Deprecated
  @Inject
  public NetworkService(
      @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory factory,
      @Parameter(NetworkServiceParameters.NetworkServicePort.class) final int nsPort,
      final NameResolver nameResolver,
      @Parameter(NetworkServiceParameters.NetworkServiceCodec.class) final Codec<T> codec,
      @Parameter(NetworkServiceParameters.NetworkServiceTransportFactory.class) final TransportFactory tpFactory,
      @Parameter(NetworkServiceParameters.NetworkServiceHandler.class) final EventHandler<Message<T>> recvHandler,
      @Parameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class) final EventHandler<Exception> exHandler,
      final LocalAddressProvider localAddressProvider) {

    this.factory = factory;
    this.codec = codec;
    this.transport = tpFactory.newInstance(nsPort,
        new LoggingEventHandler<TransportEvent>(),
        new MessageHandler<T>(recvHandler, codec, factory), exHandler);

    this.nameResolver = nameResolver;

    this.nameServiceRegisteringStage = new SingleThreadStage<>(
        "NameServiceRegisterer", new EventHandler<Tuple<Identifier, InetSocketAddress>>() {
      @Override
      public void onNext(final Tuple<Identifier, InetSocketAddress> tuple) {
        try {
          nameResolver.register(tuple.getKey(), tuple.getValue());
          LOG.log(Level.FINEST, "Registered {0} with nameservice", tuple.getKey());
        } catch (final Exception ex) {
          final String msg = "Unable to register " + tuple.getKey() + "with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);

    this.nameServiceUnregisteringStage = new SingleThreadStage<>(
        "NameServiceRegisterer", new EventHandler<Identifier>() {
      @Override
      public void onNext(final Identifier id) {
        try {
          nameResolver.unregister(id);
          LOG.log(Level.FINEST, "Unregistered {0} with nameservice", id);
        } catch (final Exception ex) {
          final String msg = "Unable to unregister " + id + " with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);
  }

  public void registerId(final Identifier id) {
    this.myId = id;
    final Tuple<Identifier, InetSocketAddress> tuple =
        new Tuple<>(id, (InetSocketAddress) this.transport.getLocalAddress());
    LOG.log(Level.FINEST, "Binding {0} to NetworkService@({1})",
        new Object[]{tuple.getKey(), tuple.getValue()});
    this.nameServiceRegisteringStage.onNext(tuple);
  }

  public void unregisterId(Identifier id) {
    this.myId = null;
    LOG.log(Level.FINEST, "Unbinding {0} to NetworkService@({1})",
        new Object[]{id, this.transport.getLocalAddress()});
    this.nameServiceUnregisteringStage.onNext(id);
  }

  public Identifier getMyId() {
    return this.myId;
  }

  public Transport getTransport() {
    return this.transport;
  }

  public Codec<T> getCodec() {
    return this.codec;
  }

  public Naming getNameClient() {
    return this.nameResolver;
  }

  public IdentifierFactory getIdentifierFactory() {
    return this.factory;
  }

  void remove(final Identifier id) {
    this.idToConnMap.remove(id);
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "Shutting down");
    this.transport.close();
    this.nameResolver.close();
  }

  @Override
  public Connection<T> newConnection(final Identifier destId) {

    if (this.myId == null) {
      throw new RuntimeException(
          "Trying to establish a connection from a Network Service that is not bound to any task");
    }

    final Connection<T> conn = this.idToConnMap.get(destId);
    if (conn != null) {
      return conn;
    }

    final Connection<T> newConnection = new NSConnection<T>(
        this.myId, destId, new LoggingLinkListener<T>(), this);

    final Connection<T> existing = this.idToConnMap.putIfAbsent(destId, newConnection);
    return existing == null ? newConnection : existing;
  }
}

class MessageHandler<T> implements EventHandler<TransportEvent> {

  private final EventHandler<Message<T>> handler;
  private final NSMessageCodec<T> codec;

  public MessageHandler(final EventHandler<Message<T>> handler,
                        final Codec<T> codec, final IdentifierFactory factory) {
    this.handler = handler;
    this.codec = new NSMessageCodec<T>(codec, factory);
  }

  @Override
  public void onNext(final TransportEvent value) {
    final byte[] data = value.getData();
    final NSMessage<T> obj = this.codec.decode(data);
    this.handler.onNext(obj);
  }
}
