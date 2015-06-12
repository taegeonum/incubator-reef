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
package org.apache.reef.io.network.temp;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.temp.impl.NetworkEvent;
import org.apache.reef.io.network.temp.impl.NetworkServiceImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;

/**
 * Created by kgw on 2015. 5. 31..
 */
@DefaultImplementation(NetworkServiceImpl.class)
public interface NetworkService extends AutoCloseable{

  public void registerId(final String networkServiceId) throws NetworkException;

  public void unregisterId() throws NetworkException;

  public <T> ConnectionPool<T> newConnectionPool(final Identifier connectionId, final Codec<T> codec,
                                                 final EventHandler<NetworkEvent<T>> eventHandler);

  public <T> ConnectionPool<T> newConnectionPool(final Identifier connectionId, final Codec<T> codec,
                                                 final EventHandler<NetworkEvent<T>> eventHandler,
                                                 final LinkListener<NetworkEvent<T>> linkListener);

  public Identifier getNetworkServiceId();

  public SocketAddress getLocalAddress();
}
