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

import org.apache.reef.io.network.temp.Connection;
import org.apache.reef.io.network.temp.ConnectionPool;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.impl.TransportEvent;


/**
 * Created by kgw on 2015. 5. 31..
 */

public class NSConnectionPool<T> implements ConnectionPool<T>, EventHandler<TransportEvent> {

  public NSConnectionPool() {

  }

  @Override
  public Identifier getConnectionId() {
    return null;
  }

  @Override
  public Connection<T> newConnection(Identifier destId) {
    return null;
  }

  public void removeConnection(Identifier destId) {
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void onNext(TransportEvent value) {

  }
}
