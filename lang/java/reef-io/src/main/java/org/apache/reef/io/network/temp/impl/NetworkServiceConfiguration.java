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


import org.apache.reef.io.network.naming.NameLookupClient;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.IdentifierFactory;

/**
 /**
 * Configuration Module Builder for NetworkService.
 */
public final class NetworkServiceConfiguration extends ConfigurationModuleBuilder {

  /**
   * The port of name server.
   */
  public static final RequiredParameter<Integer> NAME_SERVICE_PORT = new RequiredParameter<>();

  /**
   * The address of name server.
   */
  public static final RequiredParameter<String> NAME_SERVICE_ADDRESS = new RequiredParameter<>();

  /**
   * Identifier factory for the name service.
   */
  public static final RequiredParameter<IdentifierFactory> NAME_SERVER_IDENTIFIER_FACTORY = new RequiredParameter<>();

  /**
   * The port used by network service.
   */
  public static final OptionalParameter<Integer> NETWORK_SERVICE_PORT = new OptionalParameter<>();

  /**
   * Parameters for name service client
   */
  public static final OptionalParameter<Long> NAME_SERVICE_CLIENT_CACHE_TIMEOUT = new OptionalParameter<>();

  public static final OptionalParameter<Integer> NAME_SERVICE_CLIENT_RETRY_COUNT = new OptionalParameter<>();

  public static final OptionalParameter<Integer> NAME_SERVICE_CLIENT_RETRY_TIMEOUT = new OptionalParameter<>();

  /**
   * Identifier factory for the network service.
   */
  public static final RequiredParameter<IdentifierFactory> NETWORK_SERVICE_IDENDITIFER_FACTORY = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new NetworkServiceConfiguration()
      .bindNamedParameter(NameServerParameters.NameServerPort.class, NAME_SERVICE_PORT)
      .bindNamedParameter(NameServerParameters.NameServerAddr.class, NAME_SERVICE_ADDRESS)
      .bindNamedParameter(NameServerParameters.NameServerIdentifierFactory.class, NAME_SERVER_IDENTIFIER_FACTORY)
      .bindImplementation(NameClientProxy.class, NameClientRemoteProxy.class)
      .bindNamedParameter(NameLookupClient.CacheTimeout.class, NAME_SERVICE_CLIENT_CACHE_TIMEOUT)
      .bindNamedParameter(NameLookupClient.RetryCount.class, NAME_SERVICE_CLIENT_RETRY_COUNT)
      .bindNamedParameter(NameLookupClient.RetryTimeout.class, NAME_SERVICE_CLIENT_RETRY_TIMEOUT)
      .bindNamedParameter(NetworkServiceParameters.Port.class, NETWORK_SERVICE_PORT)
      .bindNamedParameter(NetworkServiceParameters.IdentifierFactory.class, NETWORK_SERVICE_IDENDITIFER_FACTORY)
      .build();
}
