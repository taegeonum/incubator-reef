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

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by kgw on 2015. 5. 31..
 */
class NetworkServiceReceiveHandler implements EventHandler<TransportEvent> {

  private final ConcurrentMap<Identifier, NSConnectionPool> connectionPoolMap;
  private final Codec<NetworkServiceEvent> nsEventCodec;
  private final IdentifierFactory idFactory;

  NetworkServiceReceiveHandler(
      final ConcurrentMap<Identifier, NSConnectionPool> connectionPoolMap,
      final IdentifierFactory idFactory,
      final Codec<NetworkServiceEvent> nsEventCodec) {
    this.connectionPoolMap = connectionPoolMap;
    this.nsEventCodec = nsEventCodec;
    this.idFactory = idFactory;
  }

  @Override
  public void onNext(TransportEvent transportEvent) {
    NetworkServiceEvent decodedEvent = nsEventCodec.decode(transportEvent.getData());

  }
}
