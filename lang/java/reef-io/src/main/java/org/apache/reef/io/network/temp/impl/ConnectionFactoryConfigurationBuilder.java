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


import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

@DriverSide
@Private
public final class ConnectionFactoryConfigurationBuilder {

  public final String contextId;
  public final Class<? extends Name<String>> connectionFactoryId;
  public final Class<? extends Codec<?>> codec;
  public final Class<? extends EventHandler<?>> eventHandler;
  public final Class<? extends LinkListener<?>> linkListener;

  public ConnectionFactoryConfigurationBuilder(String contextId,
                                               Class<? extends Name<String>> connectionFactoryId,
                                               Class<? extends Codec<?>> codec,
                                               Class<? extends EventHandler<?>> eventHandler,
                                               Class<? extends LinkListener<?>> linkListener) {
    this.contextId = contextId;
    this.connectionFactoryId = connectionFactoryId;
    this.codec = codec;
    this.eventHandler = eventHandler;
    this.linkListener = linkListener;
  }

  public Configuration getConfiguration() {
    JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder();
    builder.bindNamedParameter(NetworkServiceParameters.ConnectionFactoryCodecName.class, codec.getName());
    builder.bindNamedParameter(NetworkServiceParameters.ConnectionFactoryHandlerName.class, eventHandler.getName());
    builder.bindNamedParameter(NetworkServiceParameters.ConnectionFactoryId.class, connectionFactoryId.toString());
    builder.bindNamedParameter(NetworkServiceParameters.ConnectionFactoryListenerName.class, linkListener.getName());
    return builder.build();
  }
}