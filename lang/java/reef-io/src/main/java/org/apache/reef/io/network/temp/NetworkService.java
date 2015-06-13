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
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.temp.impl.DefaultNetworkServiceImpl;
import org.apache.reef.io.network.temp.impl.NetworkEvent;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;

@DefaultImplementation(DefaultNetworkServiceImpl.class)
public interface NetworkService extends Stage {

  public <T> ConnectionFactory<T> newConnectionFactory(final Identifier clientServiceId, final Codec<T> codec,
                                                       final EventHandler<NetworkEvent<T>> eventHandler);

  public <T> ConnectionFactory<T> newConnectionFactory(final Identifier clientServiceId, final Codec<T> codec,
                                                 final EventHandler<NetworkEvent<T>> eventHandler,
                                                 final LinkListener<NetworkEvent<T>> linkListener);

  public void registerId(final Identifier nsId) throws NetworkException;

  public Identifier getNetworkServiceId();

  public SocketAddress getLocalAddress();
}