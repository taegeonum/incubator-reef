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
import org.apache.reef.io.network.naming.NameCache;
import org.apache.reef.io.network.naming.NameClient;
import org.apache.reef.io.network.naming.NameLookupClient;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.net.InetSocketAddress;

/**
 *
 */
public final class NameClientRemoteProxy implements NameClientProxy {

  private final int serverPort;
  private final NameClient nameClient;
  private Identifier localId;

  @Inject
  public NameClientRemoteProxy(
      final @Parameter(NameServerParameters.NameServerAddr.class) String serverAddr,
      final @Parameter(NameServerParameters.NameServerPort.class) int serverPort,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory factory,
      final @Parameter(NameLookupClient.CacheTimeout.class) long timeout,
      final @Parameter(NameLookupClient.RetryCount.class) int retryCount,
      final @Parameter(NameLookupClient.RetryTimeout.class) int retryTimeout,
      final LocalAddressProvider addressProvider) {
    this.serverPort = serverPort;
    this.nameClient = new NameClient(serverAddr, serverPort, factory, retryCount,
        retryTimeout, new NameCache(timeout), addressProvider);
  }

  @Override
  public Identifier getLocalIdentifier() {
    return localId;
  }

  @Override
  public int getNameServerPort() {
    return serverPort;
  }

  @Override
  public void registerId(Identifier id, InetSocketAddress address) throws NetworkException {
    try {
      nameClient.register(id, address);
      localId = id;
    } catch(Exception e) {
      throw new NetworkException(e);
    }
  }

  @Override
  public void unregisterId() throws NetworkException {
    try {
      nameClient.unregister(localId);
      localId = null;
    } catch(Exception e) {
      throw new NetworkException(e);
    }
  }

  @Override
  public void close() throws Exception {
    nameClient.close();
  }

  @Override
  public InetSocketAddress lookup(Identifier id) throws Exception {
    return nameClient.lookup(id);
  }
}
