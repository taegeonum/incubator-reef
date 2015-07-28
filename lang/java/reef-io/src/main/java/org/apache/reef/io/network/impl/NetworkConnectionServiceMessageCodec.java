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

import org.apache.reef.io.network.avro.AvroNetworkConnectionServiceMessage;
import org.apache.reef.io.network.util.AvroUtils;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * DefaultNetworkMessageCodec implementation.
 * This codec encodes/decodes NetworkConnectionServiceMessageImpl according to the type <T>.
 */
final class NetworkConnectionServiceMessageCodec implements Codec<NetworkConnectionServiceMessage> {

  private final IdentifierFactory factory;
  /**
   * Contains entries of (id of connection factory, instance of connection factory).
   */
  private final Map<String, NetworkConnectionFactory> connFactoryMap;

  /**
   * Constructs a network connection service message codec.
   */
  NetworkConnectionServiceMessageCodec(
      final IdentifierFactory factory,
      final Map<String, NetworkConnectionFactory> connFactoryMap) {
    this.factory = factory;
    this.connFactoryMap = connFactoryMap;
  }

  /**
   * Encodes a network connection service message to bytes.
   * @param obj a message
   * @return bytes
   */
  @Override
  public byte[] encode(final NetworkConnectionServiceMessage obj) {
    final NetworkConnectionFactory connFactory = connFactoryMap.get(obj.getConnectionFactoryId());
    final Codec codec = connFactory.getCodec();
    boolean isStreamingCodec = connFactory.getIsStreamingCodec();
    final AvroNetworkConnectionServiceMessage.Builder mBuilder =
        AvroNetworkConnectionServiceMessage.newBuilder()
            .setConnectionFactoryId(obj.getConnectionFactoryId())
            .setSrcId(obj.getSrcId().toString())
            .setDestId(obj.getDestId().toString())
            .setDataSize(obj.getData().size());
    final List<ByteBuffer> bBuffers = new LinkedList<>();

    if (isStreamingCodec) {
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        try (final DataOutputStream daos = new DataOutputStream(baos)) {
          for (final Object rec : obj.getData()) {
            ((StreamingCodec) codec).encodeToStream(rec, daos);
          }
          bBuffers.add(ByteBuffer.wrap(baos.toByteArray()));
        }
      } catch (final IOException e) {
        throw new RuntimeException("IOException", e);
      }
    } else {
      for (final Object message : obj.getData()) {
        bBuffers.add(ByteBuffer.wrap(codec.encode(message)));
      }
    }
    mBuilder.setDataList(bBuffers);
    return AvroUtils.toBytes(mBuilder.build(), AvroNetworkConnectionServiceMessage.class);
  }

  /**
   * Decodes a network connection service message from bytes.
   *
   * @param data bytes
   * @return a message
   */
  @Override
  public NetworkConnectionServiceMessage decode(final byte[] data) {
    final AvroNetworkConnectionServiceMessage avroNetworkConnectionServiceMessage
        = AvroUtils.fromBytes(data, AvroNetworkConnectionServiceMessage.class);
    final String connFactoryId = avroNetworkConnectionServiceMessage.getConnectionFactoryId().toString();
    final Identifier srcId = factory.getNewInstance(avroNetworkConnectionServiceMessage.getSrcId().toString());
    final Identifier destId = factory.getNewInstance(avroNetworkConnectionServiceMessage.getDestId().toString());
    final int size = avroNetworkConnectionServiceMessage.getDataSize();
    final List list = new ArrayList(size);
    final NetworkConnectionFactory connFactory = connFactoryMap.get(connFactoryId);
    final Codec codec = connFactory.getCodec();
    final boolean isStreamingCodec = connFactory.getIsStreamingCodec();
    final List<ByteBuffer> byteBuffers = avroNetworkConnectionServiceMessage.getDataList();

    if (isStreamingCodec) {
      final byte[] bytes = byteBuffers.get(0).array();
      try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
        try (final DataInputStream dais = new DataInputStream(bais)) {
          for (int i = 0; i < size; i++) {
            list.add(((StreamingCodec) codec).decodeFromStream(dais));
          }
        }
      } catch (final IOException e) {
        throw new RuntimeException("IOException", e);
      }
    } else {
      for (ByteBuffer byteBuffer : byteBuffers) {
        list.add(codec.decode(byteBuffer.array()));
      }
    }

    return new NetworkConnectionServiceMessage(
        connFactoryId,
        srcId,
        destId,
        list
    );
  }
}