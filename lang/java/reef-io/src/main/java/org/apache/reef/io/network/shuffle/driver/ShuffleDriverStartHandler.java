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
package org.apache.reef.io.network.shuffle.driver;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.ns.*;
import org.apache.reef.io.network.shuffle.params.ShuffleNetworkServiceId;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;

/**
 *
 */
final class ShuffleDriverStartHandler implements EventHandler<StartTime> {

  private final NetworkService networkService;
  private final IdentifierFactory idFactory;
  private final ShuffleMessageCodec codec;
  private final ShuffleMessageHandler handler;
  private final ShuffleLinkListener linkListener;

  @Inject
  public ShuffleDriverStartHandler(
      final NetworkService networkService,
      final ShuffleMessageCodec codec,
      final ShuffleMessageHandler handler,
      final ShuffleLinkListener linkListener,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory) {
    this.networkService = networkService;
    this.codec = codec;
    this.handler = handler;
    this.linkListener = linkListener;
    this.idFactory = idFactory;
  }

  @Override
  public void onNext(final StartTime value) {
    // TODO : It should be removed by including an API setting driver network service id through NetworkService driverConfiguration.
    networkService.registerId(idFactory.getNewInstance(ShuffleDriverConfiguration.SHUFFLE_DRIVER_IDENTIFIER));
    try {
      networkService.registerConnectionFactory(ShuffleNetworkServiceId.class, codec, handler, linkListener);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }
}
