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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.Identifier;

import java.net.SocketAddress;
import java.util.List;


/**
 * DefaultNSMessage implementation.
 * This is used for messages of NetworkServiceClient.
 */
final class DefaultNSMessage<T> implements Message<T> {

  private final List<T> messages;
  private SocketAddress remoteAddr;
  private final String connFactoryId;
  private final Identifier srcId;
  private final Identifier remoteId;

  /**
   * Constructs a network service message.
   *
   * @param connFactoryId the connection factory identifier
   * @param srcId      the source identifier
   * @param remoteId   the remote identifier
   * @param messages  the list of messages
   */
  public DefaultNSMessage(
      final String connFactoryId,
      final Identifier srcId,
      final Identifier remoteId,
      final List<T> messages) {
    this.connFactoryId = connFactoryId;
    this.srcId = srcId;
    this.remoteId = remoteId;
    this.messages = messages;
  }

  void setRemoteAddress(final SocketAddress remoteAddress) {
    this.remoteAddr = remoteAddress;
  }

  /**
   * Gets a remote identifier.
   *
   * @return a remote id
   */
  @Override
  public Identifier getDestId() {
    return remoteId;
  }

  /**
   * Gets a connection identifier.
   *
   * @return a connection factory id
   */
  public String getConnectionFactoryId() {
    return connFactoryId;
  }


  /**
   * Gets a source identifier.
   *
   * @return a source id
   */
  @Override
  public Identifier getSrcId() {
    return srcId;
  }

  @Override
  public List<T> getData() {
    return messages;
  }

  /**
   * Returns a string representation of this object.
   *
   * @return a string representation of this object
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("NSMessage");
    builder.append(" remoteID=");
    builder.append(remoteId);
    builder.append(" message=[| ");
    for (T message : messages) {
      builder.append(message + " |");
    }
    builder.append("]");
    return builder.toString();
  }
}