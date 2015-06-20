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
package org.apache.reef.io.data.loading.api;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.io.data.loading.impl.EvaluatorRequestSerializer;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver component for the DataLoadingService
 * Also acts as the central point for resource requests
 * All the allocated evaluators pass through this and
 * the ones that need data loading have a context stacked
 * that enables a task to get access to Data via the
 * {@link DataSet}.
 * <p/>
 * TODO: Add timeouts
 */
@DriverSide
@Unit
public class DataLoader {

  private static final Logger LOG = Logger.getLogger(DataLoader.class.getName());

  private final ConcurrentMap<String, Pair<Configuration, Configuration>> submittedDataEvalConfigs = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Configuration> submittedComputeEvalConfigs = new ConcurrentHashMap<>();
  private final BlockingQueue<Configuration> failedComputeEvalConfigs = new LinkedBlockingQueue<>();
  private final BlockingQueue<Pair<Configuration, Configuration>> failedDataEvalConfigs = new LinkedBlockingQueue<>();

  private final AtomicInteger numComputeRequestsToSubmit = new AtomicInteger(0);

  private final DataLoadingService dataLoadingService;
  private final int dataEvalMemoryMB;
  private final int dataEvalCore;
  private final EvaluatorRequest computeRequest;
  private final SingleThreadStage<EvaluatorRequest> resourceRequestStage;
  private final ResourceRequestHandler resourceRequestHandler;
  private final int computeEvalMemoryMB;
  private final int computeEvalCore;
  private final EvaluatorRequestor requestor;

  @Inject
  public DataLoader(
      final Clock clock,
      final EvaluatorRequestor requestor,
      final DataLoadingService dataLoadingService,
      @Parameter(DataLoadingRequestBuilder.DataLoadingEvaluatorMemoryMB.class) final int dataEvalMemoryMB,
      @Parameter(DataLoadingRequestBuilder.DataLoadingEvaluatorNumberOfCores.class) final int dataEvalCore,
      @Parameter(DataLoadingRequestBuilder.DataLoadingComputeRequest.class) final String serializedComputeRequest) {

    // FIXME: Issue #855: We need this alarm to look busy for REEF.
    clock.scheduleAlarm(30000, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm time) {
        LOG.log(Level.FINE, "Received Alarm: {0}", time);
      }
    });

    this.requestor = requestor;
    this.dataLoadingService = dataLoadingService;
    this.dataEvalMemoryMB = dataEvalMemoryMB;
    this.dataEvalCore = dataEvalCore;
    this.resourceRequestHandler = new ResourceRequestHandler(requestor);
    this.resourceRequestStage = new SingleThreadStage<>(this.resourceRequestHandler, 2);

    if (serializedComputeRequest.equals("NULL")) {
      this.computeRequest = null;
      this.computeEvalMemoryMB = -1;
      computeEvalCore = 1;
    } else {
      this.computeRequest = EvaluatorRequestSerializer.deserialize(serializedComputeRequest);
      this.computeEvalMemoryMB = this.computeRequest.getMegaBytes();
      this.computeEvalCore = this.computeRequest.getNumberOfCores();
      this.numComputeRequestsToSubmit.set(this.computeRequest.getNumber());

      this.resourceRequestStage.onNext(this.computeRequest);
    }

    this.resourceRequestStage.onNext(getDataLoadingRequest());
  }

  private EvaluatorRequest getDataLoadingRequest() {
    return EvaluatorRequest.newBuilder()
        .setNumber(this.dataLoadingService.getNumberOfPartitions())
        .setMemory(this.dataEvalMemoryMB)
        .setNumberOfCores(this.dataEvalCore)
        .build();
  }

  public class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      resourceRequestHandler.releaseResourceRequestGate();
    }
  }

  public class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      final String evalId = allocatedEvaluator.getId();
      LOG.log(Level.FINEST, "Allocated evaluator: {0}", evalId);

      if (!failedComputeEvalConfigs.isEmpty()) {
        LOG.log(Level.FINE, "Failed Compute requests need to be satisfied for {0}", evalId);
        final Configuration conf = failedComputeEvalConfigs.poll();
        if (conf != null) {
          LOG.log(Level.FINE, "Satisfying failed configuration for {0}", evalId);
          allocatedEvaluator.submitContext(conf);
          submittedComputeEvalConfigs.put(evalId, conf);
          return;
        }
      }

      if (!failedDataEvalConfigs.isEmpty()) {
        LOG.log(Level.FINE, "Failed Data requests need to be satisfied for {0}", evalId);
        final Pair<Configuration, Configuration> confPair = failedDataEvalConfigs.poll();
        if (confPair != null) {
          LOG.log(Level.FINE, "Satisfying failed configuration for {0}", evalId);
          allocatedEvaluator.submitContextAndService(confPair.first, confPair.second);
          submittedDataEvalConfigs.put(evalId, confPair);
          return;
        }
      }

      final int evaluatorsForComputeRequest = numComputeRequestsToSubmit.decrementAndGet();
      LOG.log(Level.FINE, "Evaluators for compute request: {0}", evaluatorsForComputeRequest);

      if (evaluatorsForComputeRequest >= 0) {
        try {
          final Configuration idConfiguration = ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER,
                  dataLoadingService.getComputeContextIdPrefix() + evaluatorsForComputeRequest)
              .build();
          LOG.log(Level.FINE, "Submitting Compute Context to {0}", evalId);
          allocatedEvaluator.submitContext(idConfiguration);
          submittedComputeEvalConfigs.put(allocatedEvaluator.getId(), idConfiguration);
          if (evaluatorsForComputeRequest == 0) {
            LOG.log(Level.FINE, "All Compute requests satisfied. Releasing gate");
            resourceRequestHandler.releaseResourceRequestGate();
          }
        } catch (final BindException e) {
          throw new RuntimeException("Unable to bind context id for Compute request", e);
        }

      } else {

        final Pair<Configuration, Configuration> confPair = new Pair<>(
            dataLoadingService.getContextConfiguration(allocatedEvaluator),
            dataLoadingService.getServiceConfiguration(allocatedEvaluator));

        LOG.log(Level.FINE, "Submitting data loading context to {0}", evalId);
        allocatedEvaluator.submitContextAndService(confPair.first, confPair.second);
        submittedDataEvalConfigs.put(allocatedEvaluator.getId(), confPair);
      }
    }
  }

  public class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {

      final String evalId = failedEvaluator.getId();

      final Configuration computeConfig = submittedComputeEvalConfigs.remove(evalId);
      if (computeConfig != null) {

        LOG.log(Level.INFO, "Received failed compute evaluator: {0}", evalId);
        failedComputeEvalConfigs.add(computeConfig);

        requestor.submit(EvaluatorRequest.newBuilder()
            .setMemory(computeEvalMemoryMB).setNumber(1).setNumberOfCores(computeEvalCore).build());

      } else {

        final Pair<Configuration, Configuration> confPair = submittedDataEvalConfigs.remove(evalId);
        if (confPair != null) {

          LOG.log(Level.INFO, "Received failed data evaluator: {0}", evalId);
          failedDataEvalConfigs.add(confPair);

          requestor.submit(EvaluatorRequest.newBuilder()
              .setMemory(dataEvalMemoryMB).setNumber(1).setNumberOfCores(dataEvalCore).build());

        } else {

          LOG.log(Level.SEVERE, "Received unknown failed evaluator " + evalId,
              failedEvaluator.getEvaluatorException());

          throw new RuntimeException("Received failed evaluator that I did not submit: " + evalId);
        }
      }
    }
  }
}
