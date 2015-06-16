/*
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
package org.apache.reef.wake.remote.transport;

import java.net.SocketAddress;

/**
 * Link listener.
 *
 * @param <T> type
 */
public interface LinkListener<T> {

  /**
   * Called when the sent message is successfully transferred.
   *
   * @param message the sent message
   */
  public void onSuccess(T message);

  /**
   * Called when the sent message to remoteAddress is failed to be transferred.
   *
   * @param cause the cause of exception
   * @param remoteAddress the exception occurred remote address
   * @param message the send message
   */
  public void onException(Throwable cause, SocketAddress remoteAddress, T message);
}
