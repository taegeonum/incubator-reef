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
package org.apache.reef.io.network.shuffle.utils;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.Identifier;

/**
 *
 */
public class SimpleNetworkMessage<T> implements Message<T> {

  private final Identifier srcId;
  private final Identifier destId;
  private final Iterable<T> data;

  public SimpleNetworkMessage(
      final Identifier srcId,
      final Identifier destId,
      final Iterable<T> data) {
    this.srcId = srcId;
    this.destId = destId;
    this.data = data;
  }

  @Override
  public Identifier getSrcId() {
    return srcId;
  }

  @Override
  public Identifier getDestId() {
    return destId;
  }

  @Override
  public Iterable<T> getData() {
    return data;
  }
}
