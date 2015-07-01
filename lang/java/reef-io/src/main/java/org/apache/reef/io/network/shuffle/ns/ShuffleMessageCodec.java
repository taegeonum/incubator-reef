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

import javax.inject.Inject;
import java.io.*;

/**
 *
 */
public final class ShuffleMessageCodec implements StreamingCodec<ShuffleMessage> {

  @Inject
  public ShuffleMessageCodec() {
  }

  @Override
  public byte[] encode(final ShuffleMessage msg) {
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
  public ShuffleMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (final DataInputStream dais = new DataInputStream(bais)) {
        return decodeFromStream(dais);
      }
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred in decode method of ShuffleMessageCodec", e);
    }
  }

  @Override
  public void encodeToStream(final ShuffleMessage msg, final DataOutputStream stream) {
    try {
      stream.writeInt(msg.getCode());
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

      final int dataLength = msg.getDataLength();
      stream.writeInt(dataLength);
      for (int i = 0; i < dataLength; i++) {
        stream.writeInt(msg.getDataAt(i).length);
        stream.write(msg.getDataAt(i));
      }
    } catch(final IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public ShuffleMessage decodeFromStream(final DataInputStream stream) {
    try {
      final int code = stream.readInt();
      String topologyName = stream.readUTF();
      String groupingName = stream.readUTF();

      if (topologyName.equals("")) {
        topologyName = null;
      }

      if (groupingName.equals("")) {
        groupingName = null;
      }

      final int dataNum = stream.readInt();
      final byte[][] serializedTuples = new byte[dataNum][];
      for (int i = 0; i < dataNum; i++) {
        final int dataSize = stream.readInt();
        final byte[] dataArr = new byte[dataSize];
        stream.readFully(dataArr);
        serializedTuples[i] = dataArr;
      }
      return new ShuffleMessage(code, topologyName, groupingName, serializedTuples);
    } catch(final IOException exception) {
      throw new RuntimeException(exception);
    }
  }
}
