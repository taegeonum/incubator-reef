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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.io.network.shuffle.grouping.Grouping;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessage;
import org.apache.reef.io.network.shuffle.params.ShuffleTopologyName;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleCodec;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class ShuffleTupleSerializerImpl<K, V> implements ShuffleTupleSerializer<K, V> {

  private final String topologyName;
  private final GroupingDescription<K, V> groupingDescription;
  private final NodePoolDescription receiverPoolDescription;
  private final Grouping<K> grouping;
  private final Codec<Tuple<K, V>> tupleCodec;

  @Inject
  public ShuffleTupleSerializerImpl(
      final @Parameter(ShuffleTopologyName.class) String topologyName,
      final @Parameter(ShuffleTupleCodec.class) Codec<Tuple<K, V>> tupleCodec,
      final NodePoolDescription receiverPoolDescription,
      final GroupingDescription<K, V> groupingDescription,
      final Grouping<K> grouping) {
    this.topologyName = topologyName;
    this.tupleCodec = tupleCodec;
    this.groupingDescription = groupingDescription;
    this.receiverPoolDescription = receiverPoolDescription;
    this.grouping = grouping;
  }

  @Override
  public List<Tuple<String, ShuffleMessage>> serializeTuple(final Tuple<K, V> tuple) {
    final byte[][] serializedTuple = new byte[][] { tupleCodec.encode(tuple) };
    return serializeTupleWithData(tuple.getKey(), serializedTuple);
  }

  @Override
  public List<Tuple<String, ShuffleMessage>> serializeTuple(final K key, final List<V> valueList) {
    final byte[][] serializedTuples = new byte[valueList.size()][];

    int i = 0;
    for (V value : valueList) {
      serializedTuples[i++] = tupleCodec.encode(new Tuple<>(key, value));
    }

    return serializeTupleWithData(key, serializedTuples);
  }

  private List<Tuple<String, ShuffleMessage>> serializeTupleWithData(final K key, final byte[][] data) {
    final List<String> nodeIdList = grouping.selectReceivers(key, receiverPoolDescription);
    final List<Tuple<String, ShuffleMessage>> messageList = new ArrayList<>(nodeIdList.size());
    for (final String nodeId : nodeIdList) {
      messageList.add(new Tuple<>(
          nodeId,
          createShuffleMessage(data)
      ));
    }

    return messageList;
  }

  @Override
  public List<Tuple<String, ShuffleMessage>> serializeTupleList(final List<Tuple<K, V>> tupleList) {
    final Map<String, List<byte[]>> serializedTupleDataMap = new HashMap<>();
    for (final Tuple<K, V> tuple : tupleList) {
      final byte[] serializedTuple = tupleCodec.encode(tuple);
      for (final String nodeId : grouping.selectReceivers(tuple.getKey(), receiverPoolDescription)) {
        if (!serializedTupleDataMap.containsKey(nodeId)) {
          serializedTupleDataMap.put(nodeId, new ArrayList<byte[]>());
        }
        serializedTupleDataMap.get(nodeId).add(serializedTuple);
      }
    }

    final List<Tuple<String, ShuffleMessage>> serializedTupleList = new ArrayList<>(serializedTupleDataMap.size());
    for (Map.Entry<String, List<byte[]>> entry : serializedTupleDataMap.entrySet()) {
      int i = 0;
      final byte[][] data = new byte[entry.getValue().size()][];
      for (final byte[] serializedTuple : entry.getValue()) {
        data[i] = serializedTuple;
      }
      serializedTupleList.add(new Tuple<>(entry.getKey(), createShuffleMessage(data)));
    }
    return serializedTupleList;
  }

  private ShuffleMessage createShuffleMessage(final byte[][] data) {
    return new ShuffleMessage(ShuffleMessage.TUPLE, topologyName, groupingDescription.getGroupingName(), data);
  }
}
