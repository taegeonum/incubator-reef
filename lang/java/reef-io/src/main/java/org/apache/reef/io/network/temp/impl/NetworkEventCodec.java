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

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


final class NetworkEventCodec implements Codec<NetworkEvent> {

  private final IdentifierFactory factory;
  private final Map<Identifier, NSConnectionFactory> connectionFactoryMap;
  private final Map<Identifier, Boolean> isStreamingCodecMap;

  NetworkEventCodec(
      final IdentifierFactory factory,
      final Map<Identifier, NSConnectionFactory> connectionFactoryMap,
      final Map<Identifier, Boolean> isStreamingCodecMap) {

    this.factory = factory;
    this.connectionFactoryMap = connectionFactoryMap;
    this.isStreamingCodecMap = isStreamingCodecMap;
  }

  @Override
  public NetworkEvent decode(byte[] data) {

    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        final Identifier clientId = factory.getNewInstance(dais.readUTF());
        final Identifier srcId = factory.getNewInstance(dais.readUTF());
        final Identifier destId = factory.getNewInstance(dais.readUTF());
        final int size = dais.readInt();
        final List list = new ArrayList(size);
        final boolean isStreamingCodec = isStreamingCodecMap.get(clientId);
        final Codec codec = connectionFactoryMap.get(clientId).getCodec();

        if (isStreamingCodec) {
          for (int i = 0; i < size; i++) {
            list.add(((StreamingCodec) codec).decodeFromStream(dais));
          }
        } else {
          for (int i = 0; i < size; i++) {
            int byteSize = dais.readInt();
            byte[] bytes = new byte[byteSize];
            dais.read(bytes);
            list.add(codec.decode(bytes));
          }
        }

        return new NetworkEvent(
            clientId,
            srcId,
            destId,
            list
        );
      }
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

  @Override
  public byte[] encode(NetworkEvent obj) {
    final Codec codec = connectionFactoryMap.get(obj.getClientId()).getCodec();
    final Boolean isStreamingCodec = isStreamingCodecMap.get(obj.getClientId());

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream daos = new DataOutputStream(baos)) {
        daos.writeUTF(obj.getClientId().toString());
        daos.writeUTF(obj.getSrcId().toString());
        daos.writeUTF(obj.getDestId().toString());
        daos.writeInt(obj.getData().size());

        if (isStreamingCodec) {
          for (final Object rec : obj.getData()) {
            ((StreamingCodec) codec).encodeToStream(rec, daos);
          }
        } else {
          final Iterable dataList = obj.getData();
          for (Object event : dataList) {
            byte[] bytes = codec.encode(event);
            daos.writeInt(bytes.length);
            daos.write(bytes);
          }
        }
        return baos.toByteArray();
      }
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }
}
