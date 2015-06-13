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
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.temp.NameClientProxy;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.net.InetSocketAddress;

/**
 * NameClient proxy for local name server
 */
public final class NameClientLocalProxy implements NameClientProxy{

  private final NameServer nameServer;


  @Inject
  public NameClientLocalProxy(
      final NameServer nameServer) {
    this.nameServer = nameServer;
  }

  @Override
  public int getNameServerPort() {
    return nameServer.getPort();
  }

  @Override
  public synchronized void registerId(Identifier id, InetSocketAddress address) throws NetworkException {
    nameServer.register(id, address);
  }

  @Override
  public synchronized void unregisterId(Identifier id) throws NetworkException {
    nameServer.unregister(id);
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public InetSocketAddress lookup(Identifier id) throws Exception {
    return nameServer.lookup(id);
  }
}
