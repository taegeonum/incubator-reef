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

import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.Map;

/**
 *
 */
final class NetworkServiceLinkListener implements LinkListener<NetworkEvent> {

  private final Map<String, NSConnectionFactory> connectionFactoryMap;

  NetworkServiceLinkListener(
      final Map<String, NSConnectionFactory> connectionFactoryMap) {
    this.connectionFactoryMap = connectionFactoryMap;
  }

  @Override
  public void onSuccess(NetworkEvent message) {
    final LinkListener listener = connectionFactoryMap.get(message.getClientId()).getLinkListener();
    if (listener != null) {
      listener.onSuccess(message);
    }

  }

  @Override
  public void onException(Throwable cause, SocketAddress remoteAddress, NetworkEvent message) {
    final LinkListener listener = connectionFactoryMap.get(message.getClientId()).getLinkListener();
    if (listener != null) {
      listener.onException(cause, remoteAddress, message);
    }
  }
}
