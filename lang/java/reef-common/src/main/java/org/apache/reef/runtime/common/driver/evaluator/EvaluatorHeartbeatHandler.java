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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives heartbeats from all Evaluators and dispatches them to the right EvaluatorManager instance.
 */
@Private
@DriverSide
public final class EvaluatorHeartbeatHandler
    implements EventHandler<RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto>> {
  private static final Logger LOG = Logger.getLogger(EvaluatorHeartbeatHandler.class.getName());
  private final Evaluators evaluators;
  private final EvaluatorManagerFactory evaluatorManagerFactory;

  @Inject
  EvaluatorHeartbeatHandler(final Evaluators evaluators, final EvaluatorManagerFactory evaluatorManagerFactory) {
    this.evaluators = evaluators;
    this.evaluatorManagerFactory = evaluatorManagerFactory;
  }

  @Override
  public void onNext(final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatMessage) {
    final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeat = evaluatorHeartbeatMessage.getMessage();
    final ReefServiceProtos.EvaluatorStatusProto status = heartbeat.getEvaluatorStatus();
    final String evaluatorId = status.getEvaluatorId();

    LOG.log(Level.FINEST, "TIME: Begin Heartbeat {0}", evaluatorId);
    LOG.log(Level.FINEST, "Heartbeat from Evaluator {0} with state {1} timestamp {2} from remoteId {3}",
        new Object[]{evaluatorId, status.getState(), heartbeat.getTimestamp(),
            evaluatorHeartbeatMessage.getIdentifier()});

    final Optional<EvaluatorManager> evaluatorManager = this.evaluators.get(evaluatorId);
    if (evaluatorManager.isPresent()) {
      evaluatorManager.get().onEvaluatorHeartbeatMessage(evaluatorHeartbeatMessage);
    } else {
      final StringBuilder message = new StringBuilder("Contact from unknown Evaluator with identifier '");
      message.append(evaluatorId);
      if (heartbeat.hasEvaluatorStatus()) {
        message.append("' with state '");
        message.append(status.getState());
      }
      message.append('\'');
      throw new RuntimeException(message.toString());
    }
    LOG.log(Level.FINEST, "TIME: End Heartbeat {0}", evaluatorId);
  }
}
