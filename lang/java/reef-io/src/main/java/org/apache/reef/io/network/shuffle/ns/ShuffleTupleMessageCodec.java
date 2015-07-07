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

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 *
 */
public final class ShuffleTupleMessageCodec implements StreamingCodec<ShuffleTupleMessage> {

  private final TupleCodecMap tupleCodecMap;

  @Inject
  public ShuffleTupleMessageCodec(final TupleCodecMap tupleCodecMap) {
    this.tupleCodecMap = tupleCodecMap;
  }

  @Override
  public byte[] encode(final ShuffleTupleMessage msg) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(msg, daos);
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred in encode method of ShuffleMessageCodec", e);
    }
  }

  @Override
  public ShuffleTupleMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (final DataInputStream dais = new DataInputStream(bais)) {
        return decodeFromStream(dais);
      }
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred in decode method of ShuffleMessageCodec", e);
    }
  }

  @Override
  public void encodeToStream(final ShuffleTupleMessage msg, final DataOutputStream stream) {
    try {
      if (msg.getTopologyName() == null) {
        stream.writeUTF("");
      } else {
        stream.writeUTF(msg.getTopologyName());
      }

      if (msg.getGroupingName() == null) {
        stream.writeUTF("");
      } else {
        stream.writeUTF(msg.getGroupingName());
      }

      stream.writeInt(msg.getDataLength());

      final int messageLength = msg.getDataLength();
      final Codec<Tuple> tupleCodec = tupleCodecMap.getTupleCodec(msg.getTopologyName(), msg.getGroupingName());
      for (int i = 0; i < messageLength; i++) {
        if (tupleCodec instanceof StreamingCodec) {
          ((StreamingCodec<Tuple>)tupleCodec).encodeToStream(msg.getDataAt(i), stream);
        } else {
          final byte[] serializedTuple = tupleCodec.encode(msg.getDataAt(i));
          stream.writeInt(serializedTuple.length);
          stream.write(serializedTuple);
        }
      }
    } catch(final IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public ShuffleTupleMessage decodeFromStream(final DataInputStream stream) {
    try {
      String topologyName = stream.readUTF();
      String groupingName = stream.readUTF();

      if (topologyName.equals("")) {
        topologyName = null;
      }

      if (groupingName.equals("")) {
        groupingName = null;
      }

      final int dataNum = stream.readInt();

      final Tuple[] tupleArr = new Tuple[dataNum];
      final Codec<Tuple> tupleCodec = tupleCodecMap.getTupleCodec(topologyName, groupingName);

      for (int i = 0; i < dataNum; i++) {
        if (tupleCodec instanceof StreamingCodec) {
          tupleArr[i] = ((StreamingCodec<Tuple>)tupleCodec).decodeFromStream(stream);
        } else {
          final int length = stream.readInt();
          final byte[] serializedTuple = new byte[length];
          stream.readFully(serializedTuple);
        }
      }

      return new ShuffleTupleMessage(topologyName, groupingName, tupleArr);
    } catch(final IOException exception) {
      throw new RuntimeException(exception);
    }
  }
}
