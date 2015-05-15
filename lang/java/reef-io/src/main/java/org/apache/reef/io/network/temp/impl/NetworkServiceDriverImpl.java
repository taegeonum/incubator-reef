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
package org.apache.reef.io.network.temp.impl;


import io.netty.util.internal.ConcurrentSet;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.io.network.impl.BindNSToTask;
import org.apache.reef.io.network.impl.UnbindNSFromTask;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.io.network.temp.NetworkServiceDriver;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
public final class NetworkServiceDriverImpl implements NetworkServiceDriver {

  private static final Logger LOG = Logger.getLogger(NetworkServiceDriverImpl.class.getName());

  private final ConfigurationSerializer confSerializer;
  private final Set<ConnectionFactoryConfigurationBuilder> confBuilders;
  private final ConcurrentMap<String, Set<Class<? extends Codec<?>>>> codecMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<Class<? extends EventHandler<?>>>> eventHandlerMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<Class<? extends LinkListener<?>>>> linkListenerMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Boolean> serviceConfiguredMap;
  private final Configuration serviceConfiguration;
  private final NameServer nameServer;

  @Inject
  public NetworkServiceDriverImpl(final ConfigurationSerializer confSerializer,
                                  final NameServer server,
                                  final LocalAddressProvider localAddressProvider) {

    this.confSerializer = confSerializer;
    this.serviceConfiguredMap = new ConcurrentHashMap<>();
    this.confBuilders = new ConcurrentSet<>();
    this.nameServer = server;

    Configuration conf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, DefaultNetworkServiceImpl.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .set(ServiceConfiguration.ON_TASK_STOP, UnbindNSFromTask.class)
        .build();

    JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder(conf);
    builder.bindImplementation(NameClientProxy.class, NameClientRemoteProxy.class);
    builder.bindNamedParameter(NameServerParameters.NameServerPort.class, nameServer.getPort()+"");
    builder.bindNamedParameter(NameServerParameters.NameServerAddr.class, localAddressProvider.getLocalAddress());
    serviceConfiguration = builder.build();
  }


  @Override
  public void setConnectionFactory(String contextId,
                                   Class<? extends Name<String>> connectionFactoryId,
                                   Class<? extends Codec<?>> codec,
                                   Class<? extends EventHandler<?>> eventHandler) {
    setConnectionFactory(contextId, connectionFactoryId, codec, eventHandler, DefaultNSLinkListener.class);
  }

  @Override
  public void setConnectionFactory(String contextId,
                                   Class<? extends Name<String>> connectionFactoryId,
                                   Class<? extends Codec<?>> codec,
                                   Class<? extends EventHandler<?>> eventHandler,
                                   Class<? extends LinkListener<?>> linkListener) {

    LOG.log(Level.FINE, "Configuration connection factory for context " + contextId + ", [" + connectionFactoryId + ", "
                        + codec + ", " + eventHandler + ", " + linkListener);

    addClassToMap(contextId, codecMap, codec);
    addClassToMap(contextId, eventHandlerMap, eventHandler);
    addClassToMap(contextId, linkListenerMap, linkListener);

    confBuilders.add(new ConnectionFactoryConfigurationBuilder(contextId, connectionFactoryId,
        codec, eventHandler, linkListener));
  }

  private <T> void addClassToMap(String contextId, ConcurrentMap<String, Set<Class<? extends T>>> map, Class<? extends T> clazz) {
    Set<Class<? extends T>> set = map.get(contextId);
    if (set == null) {
      map.putIfAbsent(contextId, new ConcurrentSet<Class<? extends T>>());
    }
    set = map.get(contextId);
    set.add(clazz);
  }

  @Override
  public Configuration getServiceConfiguration(String contextId) {
    Boolean serviceConfigured = serviceConfiguredMap.putIfAbsent(contextId, true);

    if (serviceConfigured != null) {
      return Tang.Factory.getTang().newConfigurationBuilder().build();
    } else {
      JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(serviceConfiguration);

      bindSetEntryFromMap(contextId, jcb, codecMap, NetworkServiceParameters.NetworkServiceCodecs.class);
      bindSetEntryFromMap(contextId, jcb, eventHandlerMap, NetworkServiceParameters.NetworkServiceEventHandlers.class);
      bindSetEntryFromMap(contextId, jcb, linkListenerMap, NetworkServiceParameters.NetworkServiceLinkListeners.class);

      for (final ConnectionFactoryConfigurationBuilder cb : confBuilders) {
        if (cb.contextId.equals(contextId)) {
          jcb.bindSetEntry(NetworkServiceParameters.SerializedConnFactoryConfigs.class, confSerializer.toString(cb.getConfiguration()));
        }
      }

      return jcb.build();
    }
  }
  private <T> void bindSetEntryFromMap(String contextId, JavaConfigurationBuilder jcb, ConcurrentMap<String, Set<Class<? extends T>>> map, Class<? extends Name<Set<T>>> setParams) {

    Set<Class<? extends T>> codecSet = map.get(contextId);
    for (Class<? extends T> elem : codecSet) {
      jcb.bindSetEntry(setParams, elem);
    }
  }

}
