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
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.util.Map;

/**
 *
 */
final class NetworkServiceReceiveHandler implements EventHandler<TransportEvent> {

  private final Map<Identifier, NSConnectionPool> connectionPoolMap;
  private final Codec<NetworkEvent> nsEventCodec;

  NetworkServiceReceiveHandler(
      final Map<Identifier, NSConnectionPool> connectionPoolMap,
      final Codec<NetworkEvent> nsEventCodec) {
    this.connectionPoolMap = connectionPoolMap;
    this.nsEventCodec = nsEventCodec;
  }

  @Override
  public void onNext(final TransportEvent transportEvent) {
    final NetworkEvent decodedEvent = nsEventCodec.decode(transportEvent.getData());
    decodedEvent.setRemoteAddress(transportEvent.getRemoteAddress());
    final NSConnectionPool connectionPool = connectionPoolMap.get(decodedEvent.getConnectionId());
    final EventHandler eventHandler = connectionPool.getEventHandler();
    eventHandler.onNext(decodedEvent);
  }
}
