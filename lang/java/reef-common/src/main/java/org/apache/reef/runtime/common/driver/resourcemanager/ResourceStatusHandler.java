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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManagerFactory;
import org.apache.reef.runtime.common.driver.evaluator.Evaluators;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * A ResourceStatusProto message comes from the ResourceManager layer to indicate what it thinks
 * about the current state of a given resource. Ideally, we should think the same thing.
 */
@Private
public final class ResourceStatusHandler implements EventHandler<ResourceStatusEvent> {

  private final Evaluators evaluators;
  private final EvaluatorManagerFactory evaluatorManagerFactory;

  @Inject
  ResourceStatusHandler(final Evaluators evaluators, final EvaluatorManagerFactory evaluatorManagerFactory) {
    this.evaluators = evaluators;
    this.evaluatorManagerFactory = evaluatorManagerFactory;
  }

  /**
   * This resource status message comes from the ResourceManager layer; telling me what it thinks.
   * about the state of the resource executing an Evaluator; This method simply passes the message
   * off to the referenced EvaluatorManager
   *
   * @param resourceStatusEvent resource status message from the ResourceManager
   */
  @Override
  public void onNext(final ResourceStatusEvent resourceStatusEvent) {
    final Optional<EvaluatorManager> evaluatorManager = this.evaluators.get(resourceStatusEvent.getIdentifier());
    if (evaluatorManager.isPresent()) {
      evaluatorManager.get().onResourceStatusMessage(resourceStatusEvent);
    } else {
      if (resourceStatusEvent.getIsFromPreviousDriver().get()) {
        EvaluatorManager previousEvaluatorManager =
            this.evaluatorManagerFactory.createForEvaluatorFailedDuringDriverRestart(resourceStatusEvent);
        previousEvaluatorManager.onResourceStatusMessage(resourceStatusEvent);
      } else {
        throw new RuntimeException(
            "Unknown resource status from evaluator " + resourceStatusEvent.getIdentifier() +
                " with state " + resourceStatusEvent.getState()
        );
      }
    }
  }
}
