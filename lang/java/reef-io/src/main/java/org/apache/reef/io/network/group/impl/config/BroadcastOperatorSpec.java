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
package org.apache.reef.io.network.group.impl.config;

import org.apache.reef.io.network.group.api.config.OperatorSpec;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.serialization.Codec;


/**
 * The specification for the broadcast operator.
 */
public class BroadcastOperatorSpec implements OperatorSpec {
  private final String senderId;

  /**
   * Codec to be used to serialize data.
   */
  private final Class<? extends Codec> dataCodecClass;


  public BroadcastOperatorSpec(final String senderId,
                               final Class<? extends Codec> dataCodecClass) {
    super();
    this.senderId = senderId;
    this.dataCodecClass = dataCodecClass;
  }

  public String getSenderId() {
    return senderId;
  }

  @Override
  public Class<? extends Codec> getDataCodecClass() {
    return dataCodecClass;
  }

  @Override
  public String toString() {
    return "Broadcast Operator Spec: [sender=" + senderId + "] [dataCodecClass=" + Utils.simpleName(dataCodecClass)
        + "]";
  }

  public static Builder newBuilder() {
    return new BroadcastOperatorSpec.Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<BroadcastOperatorSpec> {
    private String senderId;

    private Class<? extends Codec> dataCodecClass;


    public Builder setSenderId(final String senderId) {
      this.senderId = senderId;
      return this;
    }

    public Builder setDataCodecClass(final Class<? extends Codec> codecClazz) {
      this.dataCodecClass = codecClazz;
      return this;
    }

    @Override
    public BroadcastOperatorSpec build() {
      return new BroadcastOperatorSpec(senderId, dataCodecClass);
    }
  }

}
