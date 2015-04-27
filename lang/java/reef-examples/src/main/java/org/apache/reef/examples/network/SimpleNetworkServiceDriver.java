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
package org.apache.reef.examples.network;

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.NetworkServiceConfigurationBuilderFactory;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Driver for SimpleNetworkServiceREEF.
 *
 */
@Unit
public final class SimpleNetworkServiceDriver {

  private static final Logger LOG = Logger.getLogger(SimpleNetworkServiceDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private final NetworkServiceConfigurationBuilderFactory nsConfFactory;
  private final AtomicInteger evaluatorNum = new AtomicInteger(1);
  private NetworkService networkService;
  private final IdentifierFactory identifierFactory;
  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public SimpleNetworkServiceDriver(
      final NetworkService networkService,
      final EvaluatorRequestor requestor,
      final NetworkServiceConfigurationBuilderFactory nsConfFactory) {
    this.requestor = requestor;
    this.networkService = networkService;
    this.nsConfFactory = nsConfFactory;
    this.identifierFactory = new StringIdentifierFactory();
    LOG.log(Level.FINE, "Instantiated 'SimpleNetworkServiceDriver'");
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      SimpleNetworkServiceDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting SimpleNetworkService task to AllocatedEvaluator: {0}", allocatedEvaluator);
      final int num = evaluatorNum.getAndIncrement();
      final Configuration contextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "Evaluator" + num).build();

      Configuration serviceConf = null;
      Configuration taskConf = null;
      serviceConf = nsConfFactory.createBuilder("SimplePrintEventsTask")
                .addCodec(FirstEventCodec.class)
                .addCodec(SecondEventCodec.class)
                .addEventHandler(FirstEventHandler.class)
                .addEventHandler(SecondEventHandler.class)
                .build();

      taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "SimplePrintEventsTask")
            .set(TaskConfiguration.TASK, SimplePrintEventsTask.class)
            .build();


      allocatedEvaluator.submitContextAndServiceAndTask(contextConf, serviceConf, taskConf);
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      // send events to the task
      Identifier id = identifierFactory.getNewInstance("SimplePrintEventsTask");
      networkService.sendEvent(id, new FirstEvent());
      LOG.log(Level.INFO, "Send FirstEvent");
      networkService.sendEvent(id, new SecondEvent());
      LOG.log(Level.INFO, "Send SecondEvent");
    }
  }
}
