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
package org.apache.reef.runtime.mesos.driver;

import com.google.protobuf.ByteString;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.proto.ReefServiceProtos.State;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEventImpl;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEventImpl;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.mesos.driver.parameters.MesosMasterIp;
import org.apache.reef.runtime.mesos.evaluator.REEFExecutor;
import org.apache.reef.runtime.mesos.util.EvaluatorControl;
import org.apache.reef.runtime.mesos.util.EvaluatorRelease;
import org.apache.reef.runtime.mesos.util.MesosRemoteManager;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * MesosScheduler that interacts with MesosMaster and MesosExecutors.
 */
final class REEFScheduler implements Scheduler {
  private static final Logger LOG = Logger.getLogger(REEFScheduler.class.getName());
  private static final String REEF_TAR = "reef.tar.gz";
  private static final String RUNTIME_NAME = "MESOS";
  private static final int MESOS_SLAVE_PORT = 5051; //  Assumes for now that all slaves use port 5051(default) TODO: make it configurable.
  private static final String REEF_JOB_NAME_PREFIX = "reef-job-";

  private final String reefTarUri;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;

  private final REEFEventHandlers reefEventHandlers;
  private final MesosRemoteManager mesosRemoteManager;

  private final SchedulerDriver mesosMaster;
  private final EStage<SchedulerDriver> schedulerDriverEStage;
  private final Map<String, Offer> offers = new ConcurrentHashMap<>();

  private int outstandingRequestCounter = 0;
  private final ConcurrentLinkedQueue<ResourceRequestEvent> outstandingRequests = new ConcurrentLinkedQueue<>();
  private final Map<String, ResourceRequestEvent> executorIdToLaunchedRequests = new ConcurrentHashMap<>();
  private final REEFExecutors executors;

  @Inject
  REEFScheduler(final REEFEventHandlers reefEventHandlers,
                final MesosRemoteManager mesosRemoteManager,
                final REEFExecutors executors,
                final REEFFileNames fileNames,
                final EStage<SchedulerDriver> schedulerDriverEStage,
                final ClasspathProvider classpath,
                final @Parameter(JobIdentifier.class) String jobIdentifier,
                final @Parameter(MesosMasterIp.class) String masterIp) {
    this.mesosRemoteManager = mesosRemoteManager;
    this.reefEventHandlers = reefEventHandlers;
    this.executors = executors;
    this.fileNames = fileNames;
    this.reefTarUri = getReefTarUri(jobIdentifier);
    this.classpath = classpath;
    this.schedulerDriverEStage = schedulerDriverEStage;

    final Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
        .setUser("") // TODO: make it configurable.
        .setName(REEF_JOB_NAME_PREFIX + jobIdentifier)
        .build();
    this.mesosMaster = new MesosSchedulerDriver(this, frameworkInfo, masterIp);
  }

  @Override
  public void registered(final SchedulerDriver driver,
                         final Protos.FrameworkID frameworkId,
                         final Protos.MasterInfo masterInfo) {
    LOG.log(Level.INFO, "Framework ID={0} registration succeeded", frameworkId);
  }

  @Override
  public void reregistered(final SchedulerDriver driver, final Protos.MasterInfo masterInfo) {
    LOG.log(Level.INFO, "Framework reregistered, MasterInfo: {0}", masterInfo);
  }

  /**
   * All offers in each batch of offers will be either be launched or declined.
   */
  @Override
  public void resourceOffers(final SchedulerDriver driver, final List<Protos.Offer> offers) {
    final Map<String, NodeDescriptorEventImpl.Builder> nodeDescriptorEvents = new HashMap<>();

    for (final Offer offer : offers) {
      if (nodeDescriptorEvents.get(offer.getSlaveId().getValue()) == null) {
        nodeDescriptorEvents.put(offer.getSlaveId().getValue(), NodeDescriptorEventImpl.newBuilder()
                .setIdentifier(offer.getSlaveId().getValue())
                .setHostName(offer.getHostname())
                .setPort(MESOS_SLAVE_PORT)
                .setMemorySize(getMemory(offer)));
      } else {
        final NodeDescriptorEventImpl.Builder builder = nodeDescriptorEvents.get(offer.getSlaveId().getValue());
        builder.setMemorySize(builder.build().getMemorySize() + getMemory(offer));
      }

      this.offers.put(offer.getId().getValue(), offer);
    }

    for (final NodeDescriptorEventImpl.Builder ndpBuilder : nodeDescriptorEvents.values()) {
      this.reefEventHandlers.onNodeDescriptor(ndpBuilder.build());
    }

    if (outstandingRequests.size() > 0) {
      doResourceRequest(outstandingRequests.remove());
    }
  }

  @Override
  public void offerRescinded(final SchedulerDriver driver, final Protos.OfferID offerId) {
    for (final String executorId : this.executorIdToLaunchedRequests.keySet()) {
      if (executorId.startsWith(offerId.getValue())) {
        this.outstandingRequests.add(this.executorIdToLaunchedRequests.remove(executorId));
      }
    }
  }

  @Override
  public void statusUpdate(final SchedulerDriver driver, final Protos.TaskStatus taskStatus) {
    LOG.log(Level.SEVERE, "Task Status Update:", taskStatus.toString());

    final ResourceStatusEventImpl.Builder resourceStatus =
        ResourceStatusEventImpl.newBuilder().setIdentifier(taskStatus.getTaskId().getValue());

    switch(taskStatus.getState()) {
      case TASK_STARTING:
        handleNewExecutor(taskStatus); // As there is only one Mesos Task per Mesos Executor, this is a new executor.
        return;
      case TASK_RUNNING:
        resourceStatus.setState(State.RUNNING);
        break;
      case TASK_FINISHED:
        if (taskStatus.getData().toStringUtf8().equals("eval_not_run")) { // TODO: a hack to pass closeEvaluator test, replace this with a better interface
          return;
        }
        resourceStatus.setState(State.DONE);
        break;
      case TASK_KILLED:
        resourceStatus.setState(State.KILLED);
        break;
      case TASK_LOST:
      case TASK_FAILED:
        resourceStatus.setState(State.FAILED);
        break;
      case TASK_STAGING:
        throw new RuntimeException("TASK_STAGING should not be used for status update");
      default:
        throw new RuntimeException("Unknown TaskStatus");
    }

    if (taskStatus.getMessage() != null) {
      resourceStatus.setDiagnostics(taskStatus.getMessage());
    }

    this.reefEventHandlers.onResourceStatus(resourceStatus.build());
  }

  @Override
  public void frameworkMessage(final SchedulerDriver driver,
                               final Protos.ExecutorID executorId,
                               final Protos.SlaveID slaveId,
                               final byte[] data) {
    LOG.log(Level.INFO, "Framework Message. driver: {0} executorId: {1} slaveId: {2} data: {3}",
        new Object[]{driver, executorId, slaveId, data});
  }

  @Override
  public void disconnected(final SchedulerDriver driver) {
    this.onRuntimeError(new RuntimeException("Scheduler disconnected from MesosMaster"));
  }

  @Override
  public void slaveLost(final SchedulerDriver driver, final Protos.SlaveID slaveId) {
    LOG.log(Level.SEVERE, "Slave Lost. {0}", slaveId.getValue());
  }

  @Override
  public void executorLost(final SchedulerDriver driver,
                           final Protos.ExecutorID executorId,
                           final Protos.SlaveID slaveId,
                           final int status) {
    final String diagnostics = "Executor Lost. executorid: "+executorId.getValue()+" slaveid: "+slaveId.getValue();
    final ResourceStatusEvent resourceStatus =
        ResourceStatusEventImpl.newBuilder()
            .setIdentifier(executorId.getValue())
            .setState(State.FAILED)
            .setExitCode(status)
            .setDiagnostics(diagnostics)
            .build();

    this.reefEventHandlers.onResourceStatus(resourceStatus);
  }

  @Override
  public void error(final SchedulerDriver driver, final String message) {
    this.onRuntimeError(new RuntimeException(message));
  }

  /////////////////////////////////////////////////////////////////
  // HELPER METHODS

  public void onStart() {
    this.schedulerDriverEStage.onNext(this.mesosMaster);
  }

  public void onStop() {
    this.mesosMaster.stop();
    try {
      this.schedulerDriverEStage.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void onResourceRequest(final ResourceRequestEvent resourceRequestEvent) {
    this.outstandingRequestCounter += resourceRequestEvent.getResourceCount();
    updateRuntimeStatus();
    doResourceRequest(resourceRequestEvent);
  }

  public void onResourceRelease(final ResourceReleaseEvent resourceReleaseEvent) {
    this.executors.releaseEvaluator(new EvaluatorRelease(resourceReleaseEvent.getIdentifier()));
    this.executors.remove(resourceReleaseEvent.getIdentifier());
    updateRuntimeStatus();
  }

  /**
   * Greedily acquire resources by launching a Mesos Task(w/ our custom MesosExecutor) on REEF Evaluator request.
   * Either called from onResourceRequest(for a new request) or resourceOffers(for an outstanding request).
   * TODO: reflect priority and rack/node locality specified in resourceRequestEvent.
   */
  private synchronized void doResourceRequest(final ResourceRequestEvent resourceRequestEvent) {
    int tasksToLaunchCounter = resourceRequestEvent.getResourceCount();

    for (final Offer offer : this.offers.values()) {
      final int cpuSlots = getCpu(offer) / resourceRequestEvent.getVirtualCores().get();
      final int memSlots = getMemory(offer) / resourceRequestEvent.getMemorySize().get();
      final int taskNum = Math.min(Math.min(cpuSlots, memSlots), tasksToLaunchCounter);

      if (taskNum > 0 && satisfySlaveConstraint(resourceRequestEvent, offer)) {
        final List<TaskInfo> tasksToLaunch = new ArrayList<>();
        tasksToLaunchCounter -= taskNum;

        // Launch as many MesosTasks on the same node(offer) as possible to exploit locality.
        for (int j = 0; j < taskNum; j++) {
          final String id = offer.getId().getValue() + "-" + String.valueOf(j);
          final String executorLaunchCommand = getExecutorLaunchCommand(id, resourceRequestEvent.getMemorySize().get());

          final ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
              .setExecutorId(ExecutorID.newBuilder()
                  .setValue(id)
                  .build())
              .setCommand(CommandInfo.newBuilder()
                  .setValue(executorLaunchCommand)
                  .addUris(URI.newBuilder().setValue(reefTarUri).build())
                  .build())
              .build();

          final TaskInfo taskInfo = TaskInfo.newBuilder()
              .setTaskId(TaskID.newBuilder()
                  .setValue(id)
                  .build())
              .setName(id)
              .setSlaveId(offer.getSlaveId())
              .addResources(Resource.newBuilder()
                      .setName("mem")
                      .setType(Type.SCALAR)
                      .setScalar(Value.Scalar.newBuilder()
                              .setValue(resourceRequestEvent.getMemorySize().get())
                              .build())
                      .build())
              .addResources(Resource.newBuilder()
                      .setName("cpus")
                      .setType(Type.SCALAR)
                      .setScalar(Value.Scalar.newBuilder()
                              .setValue(resourceRequestEvent.getVirtualCores().get())
                              .build())
                      .build())
              .setExecutor(executorInfo)
              .build();

          tasksToLaunch.add(taskInfo);
          this.executorIdToLaunchedRequests.put(id, resourceRequestEvent);
        }

        final Filters filters = Filters.newBuilder().setRefuseSeconds(0).build();
        mesosMaster.launchTasks(Collections.singleton(offer.getId()), tasksToLaunch, filters);
      } else {
        mesosMaster.declineOffer(offer.getId());
      }
    }

    // the offers are no longer valid(all launched or declined)
    this.offers.clear();

    // Save leftovers that couldn't be launched
    outstandingRequests.add(ResourceRequestEventImpl.newBuilder()
        .mergeFrom(resourceRequestEvent)
        .setResourceCount(tasksToLaunchCounter)
        .build());
  }

  private void handleNewExecutor(final Protos.TaskStatus taskStatus) {
    final ResourceRequestEvent resourceRequestProto =
        this.executorIdToLaunchedRequests.remove(taskStatus.getTaskId().getValue());

    final EventHandler<EvaluatorControl> evaluatorControlHandler =
        this.mesosRemoteManager.getHandler(taskStatus.getMessage(), EvaluatorControl.class);
    this.executors.add(taskStatus.getTaskId().getValue(), resourceRequestProto.getMemorySize().get(), evaluatorControlHandler);

    final ResourceAllocationEvent alloc = ResourceAllocationEventImpl.newBuilder()
        .setIdentifier(taskStatus.getTaskId().getValue())
        .setNodeId(taskStatus.getSlaveId().getValue())
        .setResourceMemory(resourceRequestProto.getMemorySize().get())
        .setVirtualCores(resourceRequestProto.getVirtualCores().get())
        .build();
    reefEventHandlers.onResourceAllocation(alloc);

    this.outstandingRequestCounter--;
    this.updateRuntimeStatus();
  }

  private synchronized void updateRuntimeStatus() {
    final RuntimeStatusEventImpl.Builder builder = RuntimeStatusEventImpl.newBuilder()
        .setName(RUNTIME_NAME)
        .setState(State.RUNNING)
        .setOutstandingContainerRequests(this.outstandingRequestCounter);

    for (final String executorId : this.executors.getExecutorIds()) {
      builder.addContainerAllocation(executorId);
    }

    this.reefEventHandlers.onRuntimeStatus(builder.build());
  }

  private void onRuntimeError(final Throwable throwable) {
    this.mesosMaster.stop();
    try {
      this.schedulerDriverEStage.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final RuntimeStatusEventImpl.Builder runtimeStatusBuilder = RuntimeStatusEventImpl.newBuilder()
        .setState(State.FAILED)
        .setName(RUNTIME_NAME);

    final Encoder<Throwable> codec = new ObjectSerializableCodec<>();
    runtimeStatusBuilder.setError(ReefServiceProtos.RuntimeErrorProto.newBuilder()
        .setName(RUNTIME_NAME)
        .setMessage(throwable.getMessage())
        .setException(ByteString.copyFrom(codec.encode(throwable)))
        .build());

    this.reefEventHandlers.onRuntimeStatus(runtimeStatusBuilder.build());
  }

  private boolean satisfySlaveConstraint(final ResourceRequestEvent resourceRequestEvent, final Offer offer) {
    return resourceRequestEvent.getNodeNameList().size() == 0 ||
        resourceRequestEvent.getNodeNameList().contains(offer.getSlaveId().getValue());
  }

  private int getMemory(final Offer offer) {
    for (final Resource resource : offer.getResourcesList()) {
      switch (resource.getName()) {
        case "mem":
          return (int)resource.getScalar().getValue();
      }
    }
    return 0;
  }

  private int getCpu(final Offer offer) {
    for (final Resource resource : offer.getResourcesList()) {
      switch (resource.getName()) {
        case "cpus":
          return (int)resource.getScalar().getValue();
      }
    }
    return 0;
  }

  private String getExecutorLaunchCommand(final String executorID, final int memorySize) {
    final String DEFAULT_JAVA_PATH = System.getenv("JAVA_HOME") + "/bin/" +  "java";
    final String classPath = "-classpath " + StringUtils.join(this.classpath.getEvaluatorClasspath(), ":");
    final String logging = "-Djava.util.logging.config.class=org.apache.reef.util.logging.Config";
    final String mesosExecutorId = "-mesos_executor_id " + executorID;

    return (new StringBuilder()
        .append(DEFAULT_JAVA_PATH + " ")
        .append("-XX:PermSize=128m" + " ")
        .append("-XX:MaxPermSize=128m" + " ")
        .append("-Xmx" + String.valueOf(memorySize) + "m" + " ")
        .append(classPath + " ")
        .append(logging + " ")
        .append(REEFExecutor.class.getName() + " ")
        .append(mesosExecutorId + " ")
        .toString());
  }

  private String getReefTarUri(final String jobIdentifier) {
    try {
      // Create REEF_TAR
      final FileOutputStream fileOutputStream = new FileOutputStream(REEF_TAR);
      final TarArchiveOutputStream tarArchiveOutputStream =
          new TarArchiveOutputStream(new GZIPOutputStream(fileOutputStream));
      final File globalFolder = new File(this.fileNames.getGlobalFolderPath());
      final DirectoryStream<Path> directoryStream = Files.newDirectoryStream(globalFolder.toPath());

      for (final Path path : directoryStream) {
        tarArchiveOutputStream.putArchiveEntry(new TarArchiveEntry(path.toFile(),
            globalFolder + "/" + path.getFileName()));

        final BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(path.toFile()));
        IOUtils.copy(bufferedInputStream, tarArchiveOutputStream);
        bufferedInputStream.close();

        tarArchiveOutputStream.closeArchiveEntry();
      }
      directoryStream.close();
      tarArchiveOutputStream.close();
      fileOutputStream.close();

      // Upload REEF_TAR to HDFS
      final FileSystem fileSystem = FileSystem.get(new Configuration());
      final org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(REEF_TAR);
      final String reefTarUri = fileSystem.getUri().toString() + "/" + jobIdentifier + "/" + REEF_TAR;
      final org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(reefTarUri);
      fileSystem.copyFromLocalFile(src, dst);

      return reefTarUri;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
