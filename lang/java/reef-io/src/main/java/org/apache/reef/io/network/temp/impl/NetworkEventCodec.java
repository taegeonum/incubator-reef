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

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.io.network.avro.AvroNetworkServiceEvent;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


final class NetworkEventCodec implements Codec<NetworkEvent> {

  private final IdentifierFactory idFactory;
  private final Map<Identifier, NSConnectionFactory> connectionFactoryMap;

  NetworkEventCodec(
      final IdentifierFactory idFactory,
      final Map<Identifier, NSConnectionFactory> connectionFactoryMap) {

    this.idFactory = idFactory;
    this.connectionFactoryMap = connectionFactoryMap;
  }

  @Override
  public NetworkEvent decode(byte[] data) {
    final AvroNetworkServiceEvent avroNetworkServiceEvent;
    final DatumReader<AvroNetworkServiceEvent> reader = new SpecificDatumReader<>(AvroNetworkServiceEvent.class);
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    try {
      avroNetworkServiceEvent = reader.read(null, decoder);
    } catch (IOException e) {
      throw new NetworkRuntimeException("Improper data was attempted to decode in NetworkEventCodec", e);
    }

    final Identifier clientId = idFactory.getNewInstance(avroNetworkServiceEvent.getClientId().toString());
    final Identifier srcId = idFactory.getNewInstance(avroNetworkServiceEvent.getSrcId().toString());
    final Identifier remoteId = idFactory.getNewInstance(avroNetworkServiceEvent.getRemoteId().toString());

    final List<ByteBuffer> byteBufferList = avroNetworkServiceEvent.getDataList();
    final List dataList = new ArrayList(byteBufferList.size());

    try {
      final Codec eventCodec = connectionFactoryMap.get(clientId).getCodec();
      for (ByteBuffer byteBuffer : byteBufferList) {
        dataList.add(eventCodec.decode(byteBuffer.array()));
      }
    } catch (NullPointerException e) {
      throw new NetworkRuntimeException("Data from " + remoteId + " to " + srcId + ":" + clientId +
          " cannot be decoded because there is no binding codec of " + clientId);
    }

    return new NetworkEvent(
        clientId,
        srcId,
        remoteId,
        dataList
    );
  }

  @Override
  public byte[] encode(NetworkEvent obj) {
    final Iterable dataList = obj.getData();
    final List<ByteBuffer> byteBufferList = new ArrayList<>(obj.getDataSize());

    final Codec eventCodec = connectionFactoryMap.get(obj.getClientId()).getCodec();

    for (Object event : dataList) {
      byteBufferList.add(ByteBuffer.wrap(eventCodec.encode(event)));
    }

    final AvroNetworkServiceEvent event = AvroNetworkServiceEvent.newBuilder()
        .setClientId(obj.getClientId().toString())
        .setSrcId(obj.getSrcId().toString())
        .setRemoteId(obj.getDestId().toString())
        .setDataList(byteBufferList)
        .build();

    final byte[] bytes;
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      final DatumWriter<AvroNetworkServiceEvent> writer = new SpecificDatumWriter<>(AvroNetworkServiceEvent.class);
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
      writer.write(event, encoder);
      encoder.flush();
      bytes = bos.toByteArray();
    } catch (IOException e) {
      throw new NetworkRuntimeException("Improper data was attempted to encode in NetworkEventCodec", e);
    }
    return bytes;
  }
}