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
public abstract class ShuffleMessage {

  private final String topologyName;
  private final String groupingName;

  public ShuffleMessage(
      final String topologyName,
      final String groupingName) {
    this.topologyName = topologyName;
    this.groupingName = groupingName;
  }

  public String getTopologyName() {
    return topologyName;
  }

  public String getGroupingName() {
    return groupingName;
  }

  public abstract int getDataLength();

  public abstract Object getDataAt(final int index);
}
