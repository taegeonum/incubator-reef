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
package org.apache.reef.io.network.impl;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.config.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default NetworkService implementation
 */
public final class DefaultNetworkServiceImpl implements NetworkService {

  private static final Logger LOG = Logger.getLogger(DefaultNetworkServiceImpl.class.getName());

  private final IdentifierFactory idFactory;
  private final NameClientProxy nameClientProxy;
  private final Transport transport;
  private final EventHandler<TransportEvent> recvHandler;
  private final ConcurrentMap<String, NSConnectionFactory> connectionFactoryMap;
  private final ConcurrentMap<String, Boolean> isStreamingCodecMap;
  private Identifier myId;
  private final Codec<NSMessage> nsCodec;
  private final LinkListener<NSMessage> nsLinkListener;


  @Inject
  private DefaultNetworkServiceImpl(
      final ConfigurationSerializer confSerializer,
      final @Parameter(NetworkServiceParameters.IdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(NetworkServiceParameters.Port.class) int nsPort,
      final @Parameter(NetworkServiceParameters.NetworkServiceCodecs.class) Set<Codec<?>> codecs,
      final @Parameter(NetworkServiceParameters.NetworkServiceEventHandlers.class) Set<EventHandler<Message<?>>> eventHandlers,
      final @Parameter(NetworkServiceParameters.NetworkServiceLinkListeners.class) Set<LinkListener<Message<?>>> linkListeners,
      final @Parameter(SerializedConnFactoryConfigs.class) Set<String> connFactoryConfigs,
      final TransportFactory transportFactory,
      final NameClientProxy nameClientProxy) throws NetworkException, IOException, InjectionException, ClassNotFoundException {

    this.idFactory = idFactory;
    this.connectionFactoryMap = new ConcurrentHashMap<>();
    this.isStreamingCodecMap = new ConcurrentHashMap<>();
    this.nsCodec = new NSMessageCodec(idFactory, connectionFactoryMap, isStreamingCodecMap);
    this.nsLinkListener = new NetworkServiceLinkListener(connectionFactoryMap);
    this.recvHandler = new NetworkServiceReceiveHandler(connectionFactoryMap, nsCodec);
    this.nameClientProxy = nameClientProxy;
    this.transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler, new DefaultNSExceptionHandler());

    for (String confString : connFactoryConfigs) {
      Configuration conf  = confSerializer.fromString(confString);
      Injector injector = Tang.Factory.getTang().newInjector(conf);

      String connectionFactoryId = injector.getNamedInstance(ConnectionFactoryId.class);
      Codec codec = findClassFromClassName(codecs, injector.getNamedInstance(ConnectionFactoryCodec.class));
      EventHandler handler = findClassFromClassName(eventHandlers, injector.getNamedInstance(ConnectionFactoryEventHandler.class));
      LinkListener listener = findClassFromClassName(linkListeners, injector.getNamedInstance(ConnectionFactoryLinkListener.class));
      connectionFactoryMap.put(connectionFactoryId, new NSConnectionFactory(this, connectionFactoryId, codec, handler, listener));
      isStreamingCodecMap.put(connectionFactoryId, codec instanceof StreamingCodec);
    }
  }

  /**
   * Find an instance of a class.
   * @param set
   * @param className
   * @return class instance
   * @throws ClassNotFoundException
   */
  private <T> T findClassFromClassName(final Set<T> set, final String className) throws ClassNotFoundException {
    Class clazz = Class.forName(className);
    for (T inst : set) {
      if (clazz.isInstance(inst)) {
        return inst;
      }
    }
    throw new RuntimeException("Instance of " + clazz + " not found in " + set);
  }



  @Override
  public void registerId(final Identifier taskId) throws NetworkException {
    LOG.log(Level.INFO, "Registering NetworkSerice " + taskId);
    this.myId = taskId;
    nameClientProxy.registerId(
        taskId,
        (InetSocketAddress) transport.getLocalAddress()
    );
  }

  /**
   * Open a channel for remoteId
   * @param remoteId
   * @throws NetworkException
   */
  <T> Link<NSMessage<T>> openLink(final Identifier remoteId) throws NetworkException {
    try {
      final SocketAddress address = nameClientProxy.lookup(remoteId);
      if (address == null) {
        throw new NetworkException("Lookup " + remoteId + " is null");
      }
      return transport.open(address, nsCodec, nsLinkListener);
    } catch(Exception e) {
      e.printStackTrace();
      throw new NetworkException(e);
    }
  }

  @Override
  public <T> ConnectionFactory<T> newConnectionFactory(final Class<? extends Name<String>> connectionFactoryId){

    ConnectionFactory<T> connFactory = connectionFactoryMap.get(connectionFactoryId.toString());

    if (connFactory == null) {
      throw new RuntimeException("Cannot find ConnectionFactory of " + connectionFactoryId + ".");
    }

    return connFactory;
  }

  @Override
  public void unregisterId(final Identifier taskId) throws NetworkException {
    nameClientProxy.unregisterId(taskId);
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
