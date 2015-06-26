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
package org.apache.reef.io.network.naming;

import org.apache.reef.io.network.naming.serialization.*;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.MultiCodec;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory to create naming codecs.
 */
final class NamingCodecFactory {

  /**
   * Creates a codec only for lookup.
   *
   * @param factory an identifier factory
   * @return a codec
   */
  static Codec<NamingMessage> createLookupCodec(IdentifierFactory factory) {
    Map<Class<? extends NamingMessage>, Codec<? extends NamingMessage>> clazzToCodecMap
        = new HashMap<Class<? extends NamingMessage>, Codec<? extends NamingMessage>>();
    clazzToCodecMap.put(NamingLookupRequest.class, new NamingLookupRequestCodec(factory));
    clazzToCodecMap.put(NamingLookupResponse.class, new NamingLookupResponseCodec(factory));
    Codec<NamingMessage> codec = new MultiCodec<NamingMessage>(clazzToCodecMap);
    return codec;
  }

  /**
   * Creates a codec only for registration.
   *
   * @param factory an identifier factory
   * @return a codec
   */
  static Codec<NamingMessage> createRegistryCodec(IdentifierFactory factory) {
    Map<Class<? extends NamingMessage>, Codec<? extends NamingMessage>> clazzToCodecMap
        = new HashMap<Class<? extends NamingMessage>, Codec<? extends NamingMessage>>();
    clazzToCodecMap.put(NamingRegisterRequest.class, new NamingRegisterRequestCodec(factory));
    clazzToCodecMap.put(NamingRegisterResponse.class, new NamingRegisterResponseCodec(new NamingRegisterRequestCodec(factory)));
    clazzToCodecMap.put(NamingUnregisterRequest.class, new NamingUnregisterRequestCodec(factory));
    Codec<NamingMessage> codec = new MultiCodec<NamingMessage>(clazzToCodecMap);
    return codec;
  }

  /**
   * Creates a codec for both lookup and registration.
   *
   * @param factory an identifier factory
   * @return a codec
   */
  static Codec<NamingMessage> createFullCodec(IdentifierFactory factory) {
    Map<Class<? extends NamingMessage>, Codec<? extends NamingMessage>> clazzToCodecMap
        = new HashMap<Class<? extends NamingMessage>, Codec<? extends NamingMessage>>();
    clazzToCodecMap.put(NamingLookupRequest.class, new NamingLookupRequestCodec(factory));
    clazzToCodecMap.put(NamingLookupResponse.class, new NamingLookupResponseCodec(factory));
    clazzToCodecMap.put(NamingRegisterRequest.class, new NamingRegisterRequestCodec(factory));
    clazzToCodecMap.put(NamingRegisterResponse.class, new NamingRegisterResponseCodec(new NamingRegisterRequestCodec(factory)));
    clazzToCodecMap.put(NamingUnregisterRequest.class, new NamingUnregisterRequestCodec(factory));
    Codec<NamingMessage> codec = new MultiCodec<NamingMessage>(clazzToCodecMap);
    return codec;
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private NamingCodecFactory() {
  }
}
