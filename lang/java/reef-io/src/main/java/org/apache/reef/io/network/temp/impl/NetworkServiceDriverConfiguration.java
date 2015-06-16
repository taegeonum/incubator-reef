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

import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.wake.IdentifierFactory;

/**
 /**
 * Configuration Module Builder for NetworkService.
 */
public final class NetworkServiceDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * The port used by name server.
   */
  public static final OptionalParameter<Integer> NAME_SERVICE_PORT = new OptionalParameter<>();


  /**
   * Identifier factory for the name service.
   */
  public static final OptionalParameter<IdentifierFactory> NAME_SERVER_IDENTIFIER_FACTORY = new OptionalParameter<>();

  /**
   * The port used by network service.
   */
  public static final OptionalParameter<Integer> NETWORK_SERVICE_PORT = new OptionalParameter<>();

  /**
   * Identifier factory for the network service.
   */
  public static final OptionalParameter<IdentifierFactory> NETWORK_SERVICE_IDENDITIFER_FACTORY = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new NetworkServiceDriverConfiguration()
      .bindNamedParameter(NameServerParameters.NameServerPort.class, NAME_SERVICE_PORT)
      .bindNamedParameter(NameServerParameters.NameServerIdentifierFactory.class, NAME_SERVER_IDENTIFIER_FACTORY)
      .bindImplementation(NameServer.class, NameServerImpl.class)
      .bindImplementation(NameClientProxy.class, NameClientLocalProxy.class)
      .bindNamedParameter(NetworkServiceParameters.Port.class, NETWORK_SERVICE_PORT)
      .bindNamedParameter(NetworkServiceParameters.IdentifierFactory.class, NETWORK_SERVICE_IDENDITIFER_FACTORY)
      .build();
}
