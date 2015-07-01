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
package org.apache.reef.io.network.shuffle.ns;

/**
 *
 */
public final class ShuffleMessage {

  public static final int TUPLE = 0;

  private final int code;
  private final String topologyName;
  private final String groupingName;
  private final byte[][] data;

  public ShuffleMessage(
      final int code,
      final String topologyName,
      final String groupingName,
      final byte[][] data) {
    this.code = code;
    this.topologyName = topologyName;
    this.groupingName = groupingName;
    this.data = data;
  }

  public int getCode() {
    return code;
  }

  public String getTopologyName() {
    return topologyName;
  }

  public String getGroupingName() {
    return groupingName;
  }

  public int getDataLength() {
    if (data == null) {
      return 0;
    }

    return data.length;
  }

  public byte[] getDataAt(final int index) {
    return data[index];
  }
}
