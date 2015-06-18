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


import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.io.network.NetworkServiceConfigurationBuilder;
import org.apache.reef.io.network.impl.*;
import org.apache.reef.io.network.naming.NameServer;
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

public final class NetworkServiceConfigurationBuilderImpl implements NetworkServiceConfigurationBuilder {

  private final NSConfigurationBuilderHelper helper;

  /**
   * NetworkServiceConfigurationBuilder for EvaluatorSide
   * @param confSerializer
   * @param server
   * @param localAddressProvider
   */
  @Inject
  private NetworkServiceConfigurationBuilderImpl(final ConfigurationSerializer confSerializer,
                                            final NameServer server,
                                            final LocalAddressProvider localAddressProvider) {

    Configuration conf1 = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, DefaultNetworkServiceImpl.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .set(ServiceConfiguration.ON_TASK_STOP, UnbindNSFromTask.class)
        .set(ServiceConfiguration.ON_CONTEXT_STOP,
            NetworkServiceClosingHandler.class)
        .build();

    Configuration conf = NetworkServiceConfiguration.CONF
        .set(NetworkServiceConfiguration.NAME_SERVICE_ADDRESS, localAddressProvider.getLocalAddress())
        .set(NetworkServiceConfiguration.NAME_SERVICE_PORT, server.getPort())
        .build();

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(conf1, conf);
    this.helper = new NSConfigurationBuilderHelper(confSerializer, cb.build());
  }

  @Override
  public void setConnectionFactory(final String contextId,
                                   final Class<? extends Name<String>> connectionFactoryId,
                                   final Class<? extends Codec<?>> codec, final Class<? extends EventHandler<?>> eventHandler,
                                   final Class<? extends LinkListener<?>> linkListener) {
    helper.setConnectionFactory(contextId, connectionFactoryId, codec, eventHandler, linkListener);
  }

  @Override
  public void setConnectionFactory(final String contextId,
                                   final Class<? extends Name<String>> connectionFactoryId,
                                   final Class<? extends Codec<?>> codec,
                                   final Class<? extends EventHandler<?>> eventHandler) {
    helper.setConnectionFactory(contextId, connectionFactoryId, codec, eventHandler, DefaultNSLinkListener.class);
  }

  @Override
  public Configuration getServiceConfiguration(final String contextId, final Class<? extends Name<String>> connectionFactoryId) {
    return helper.getServiceConfiguration(contextId, connectionFactoryId);
  }
}
