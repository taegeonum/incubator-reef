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
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.io.network.temp.NetworkService;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
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

/**
 * Default NetworkService implementation
 */
public final class DefaultNetworkServiceImpl implements NetworkService {

  private final IdentifierFactory idFactory;
  private final NameClientProxy nameClientProxy;
  private final Transport transport;
  private final EventHandler<TransportEvent> recvHandler;
  private final ConcurrentMap<String, NSConnectionFactory> connectionFactoryMap;
  private final ConcurrentMap<String, Boolean> isStreamingCodecMap;
  private Identifier myId;
  private final Codec<NetworkEvent> nsCodec;
  private final LinkListener<NetworkEvent> nsLinkListener;
  private final Set<Codec<?>> codecs;
  private final Set<EventHandler<NetworkEvent<?>>> eventHandlers;
  private final Set<LinkListener<NetworkEvent<?>>> linkListeners;

  @Inject
  public DefaultNetworkServiceImpl(
      final ConfigurationSerializer confSerializer,
      final @Parameter(NetworkServiceParameters.IdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(NetworkServiceParameters.Port.class) int nsPort,
      final @Parameter(NetworkServiceParameters.NetworkServiceCodecs.class) Set<Codec<?>> codecs,
      final @Parameter(NetworkServiceParameters.NetworkServiceEventHandlers.class) Set<EventHandler<NetworkEvent<?>>> eventHandlers,
      final @Parameter(NetworkServiceParameters.NetworkServiceLinkListeners.class) Set<LinkListener<NetworkEvent<?>>> linkListeners,
      final @Parameter(NetworkServiceParameters.SerializedConnFactoryConfigs.class) Set<String> connFactoryConfigs,
      final TransportFactory transportFactory,
      final NameClientProxy nameClientProxy) throws NetworkException, IOException, InjectionException, ClassNotFoundException {

    this.idFactory = idFactory;
    this.connectionFactoryMap = new ConcurrentHashMap<>();
    this.isStreamingCodecMap = new ConcurrentHashMap<>();
    this.nsCodec = new NetworkEventCodec(idFactory, connectionFactoryMap, isStreamingCodecMap);
    this.nsLinkListener = new NetworkServiceLinkListener(connectionFactoryMap);
    this.recvHandler = new NetworkServiceReceiveHandler(connectionFactoryMap, nsCodec);
    this.nameClientProxy = nameClientProxy;
    this.transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler, new DefaultNSExceptionHandler());
    this.codecs = codecs;
    this.eventHandlers = eventHandlers;
    this.linkListeners = linkListeners;

    for (String confString : connFactoryConfigs) {
      Configuration conf  = confSerializer.fromString(confString);
      Injector injector = Tang.Factory.getTang().newInjector(conf);

      String connectionFactoryId = injector.getNamedInstance(NetworkServiceParameters.ConnectionFactoryId.class);
      Codec codec = findClassFromClassName(codecs, injector.getNamedInstance(NetworkServiceParameters.ConnectionFactoryCodecName.class));
      EventHandler handler = findClassFromClassName(eventHandlers, injector.getNamedInstance(NetworkServiceParameters.ConnectionFactoryHandlerName.class));
      LinkListener listener = findClassFromClassName(linkListeners, injector.getNamedInstance(NetworkServiceParameters.ConnectionFactoryListenerName.class));
      connectionFactoryMap.put(connectionFactoryId, new NSConnectionFactory(this, connectionFactoryId, codec, handler, listener));
      isStreamingCodecMap.put(connectionFactoryId, codec instanceof StreamingCodec);
    }
  }

  private <T> T findClassFromClassName(Set<T> set, String className) throws ClassNotFoundException {
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
    this.myId = taskId;
    nameClientProxy.registerId(
        taskId,
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
  public <T> ConnectionFactory<T> newConnectionFactory(final Class<? extends Name<String>> clientServiceName) {

    ConnectionFactory<T> connFactory = connectionFactoryMap.get(clientServiceName.toString());

    if (connFactory == null) {
      throw new NetworkRuntimeException("ConnectionFactory of " + clientServiceName + " is not binded.");
    }

    return connFactory;
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
