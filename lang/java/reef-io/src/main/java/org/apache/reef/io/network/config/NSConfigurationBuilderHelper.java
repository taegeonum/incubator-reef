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
package org.apache.reef.io.network.config;


import io.netty.util.internal.ConcurrentSet;
import org.apache.reef.io.network.config.parameters.NetworkServiceParameters;
import org.apache.reef.io.network.config.parameters.SerializedConnFactoryConfigs;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

final class NSConfigurationBuilderHelper {

  private static final Logger LOG = Logger.getLogger(NSConfigurationBuilderHelper.class.getName());

  private final ConfigurationSerializer confSerializer;
  private final Set<ConnectionFactoryConfig> confBuilders;
  private final ConcurrentMap<String, Set<Class<? extends Codec<?>>>> codecMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<Class<? extends Codec<?>>>> configuredCodecMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Set<Class<? extends EventHandler<?>>>> eventHandlerMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<Class<? extends EventHandler<?>>>> configuredEventHandlerMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Set<Class<? extends LinkListener<?>>>> linkListenerMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<Class<? extends LinkListener<?>>>> configuredLinkListenerMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Boolean> serviceConfiguredMap;
  private Configuration serviceConfiguration;

  /**
   * NetworkService configuration helper
   */
  NSConfigurationBuilderHelper(final ConfigurationSerializer confSerializer,
                               final Configuration initialConf) {

    this.confSerializer = confSerializer;
    this.serviceConfiguredMap = new ConcurrentHashMap<>();
    this.confBuilders = new ConcurrentSet<>();
    this.serviceConfiguration = initialConf;
  }

  public void setConnectionFactory(final String contextId,
                                   final Class<? extends Name<String>> connectionFactoryId,
                                   final Class<? extends Codec<?>> codec,
                                   final Class<? extends EventHandler<?>> eventHandler,
                                   final Class<? extends LinkListener<?>> linkListener) {

    LOG.log(Level.FINE, "Sets configuration of connection factory for the context " + contextId + ", [" + connectionFactoryId + ", "
                        + codec + ", " + eventHandler + ", " + linkListener);

    validation(contextId, connectionFactoryId);
    addClassToMap(contextId, codecMap, codec);
    addClassToMap(contextId, eventHandlerMap, eventHandler);
    addClassToMap(contextId, linkListenerMap, linkListener);

    confBuilders.add(new ConnectionFactoryConfig(contextId, connectionFactoryId,
        codec, eventHandler, linkListener));
  }

  /**
   * Check duplicated connectionFactoryId
   * @param contextId
   * @param connectionFactoryId
   */
  private void validation(final String contextId, final Class<? extends Name<String>> connectionFactoryId) {
    for (ConnectionFactoryConfig cb : confBuilders) {
      if (cb.contextId.equals(contextId) && cb.connectionFactoryId.equals(connectionFactoryId)) {
        throw new RuntimeException("The connection Factory of " + connectionFactoryId + " already exists.");
      }
    }
  }

  private <T> void addClassToMap(final String contextId,
                                 final ConcurrentMap<String, Set<Class<? extends T>>> map,
                                 final Class<? extends T> clazz) {
    Set<Class<? extends T>> set = map.get(contextId);
    if (set == null) {
      map.putIfAbsent(contextId, new ConcurrentSet<Class<? extends T>>());
    }
    set = map.get(contextId);
    set.add(clazz);
  }

  public synchronized Configuration getServiceConfiguration(final String contextId,
                                               final Class<? extends Name<String>> connectionFactoryId) {

    int find = 0;
    Boolean serviceConfigured = serviceConfiguredMap.putIfAbsent(contextId, true);
    JavaConfigurationBuilder jcb;
    if (serviceConfigured != null) {
      // prevent duplicated configuration
      jcb = Tang.Factory.getTang().newConfigurationBuilder();
    } else {
      jcb = Tang.Factory.getTang().newConfigurationBuilder(serviceConfiguration);
    }

    for (final ConnectionFactoryConfig cb : confBuilders) {
      if (cb.contextId.equals(contextId) && cb.connectionFactoryId.equals(connectionFactoryId)) {
        if (!elemExists(contextId, configuredCodecMap, cb.codec)) {
          addClassToMap(contextId, configuredCodecMap, cb.codec);
          jcb.bindSetEntry(NetworkServiceParameters.NetworkServiceCodecs.class, cb.codec);
        }
        if (!elemExists(contextId, configuredEventHandlerMap, cb.eventHandler)) {
          addClassToMap(contextId, configuredEventHandlerMap, cb.eventHandler);
          jcb.bindSetEntry(NetworkServiceParameters.NetworkServiceEventHandlers.class, cb.eventHandler);
        }
        if (!elemExists(contextId, configuredLinkListenerMap, cb.linkListener)) {
          addClassToMap(contextId, configuredLinkListenerMap, cb.linkListener);
          jcb.bindSetEntry(NetworkServiceParameters.NetworkServiceLinkListeners.class, cb.linkListener);
        }
        jcb.bindSetEntry(SerializedConnFactoryConfigs.class, confSerializer.toString(cb.getConfiguration()));
        find = 1;
        break;
      }
    }

    if (find == 0) {
      throw new RuntimeException("Cannot find NetworkServiceConfiguration of " + contextId + ":" + connectionFactoryId);
    }
    return jcb.build();
  }

  /**
   * Prevent duplicated configuration
   */
  private <T> boolean elemExists(final String contextId,
                                 final ConcurrentMap<String, Set<Class<? extends T>>> map,
                                 final Class<? extends T> elem) {
    Set<Class<? extends T>> set = map.get(contextId);

    if (set == null) {
      return false;
    } else {
      return set.contains(elem);
    }
  }

}
