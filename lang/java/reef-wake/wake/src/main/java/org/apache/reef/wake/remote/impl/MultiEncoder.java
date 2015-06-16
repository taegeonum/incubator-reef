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
package org.apache.reef.wake.remote.impl;

import com.google.protobuf.ByteString;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.proto.WakeRemoteProtos.WakeTuplePBuf;

import java.util.Map;

/**
 * Encoder using the WakeTuple protocol buffer.
 * (class name and bytes)
 *
 * @param <T>
 */
public class MultiEncoder<T> implements Encoder<T> {

  private final Map<Class<? extends T>, Encoder<? extends T>> clazzToEncoderMap;

  /**
   * Constructs an encoder that encodes an object to bytes based on the class name.
   *
   * @param clazzToDecoderMap
   */
  public MultiEncoder(Map<Class<? extends T>, Encoder<? extends T>> clazzToEncoderMap) {
    this.clazzToEncoderMap = clazzToEncoderMap;
  }

  /**
   * Encodes an object to a byte array.
   *
   * @param obj
   */
  @Override
  public byte[] encode(T obj) {
    Encoder<T> encoder = (Encoder<T>) clazzToEncoderMap.get(obj.getClass());
    if (encoder == null)
      throw new RemoteRuntimeException("Encoder for " + obj.getClass() + " not known.");

    WakeTuplePBuf.Builder tupleBuilder = WakeTuplePBuf.newBuilder();
    tupleBuilder.setClassName(obj.getClass().getName());
    tupleBuilder.setData(ByteString.copyFrom(encoder.encode(obj)));
    return tupleBuilder.build().toByteArray();
  }

}
