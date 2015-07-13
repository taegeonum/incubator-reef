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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessageCodec;
import org.apache.reef.io.network.group.impl.task.GroupCommNetworkHandlerImpl;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register GroupComm connection factory to NetworkConnectionService.
 */
public final class GroupCommConnFactoryRegisterHandler implements EventHandler<ContextStart> {

  private final IdentifierFactory idfac;
  private final NetworkConnectionService ncs;
  private final GroupCommunicationMessageCodec codec;
  private final GroupCommNetworkHandlerImpl handler;
  private final GroupCommLinkListener listener;

  @Inject
  private GroupCommConnFactoryRegisterHandler(
      @Parameter(NetworkConnectionServiceIdFactory.class) final IdentifierFactory idfac,
      final NetworkConnectionService ncs,
      final GroupCommunicationMessageCodec codec,
      final GroupCommNetworkHandlerImpl handler,
      final GroupCommLinkListener listener) {
    this.idfac = idfac;
    this.ncs = ncs;
    this.codec = codec;
    this.handler = handler;
    this.listener = listener;
  }

  @Override
  public void onNext(ContextStart value) {
    try {
      ncs.registerConnectionFactory(idfac.getNewInstance(GroupCommDriverImpl.GROUP_COMM_NCS_ID),
          codec, handler, listener);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }
}
