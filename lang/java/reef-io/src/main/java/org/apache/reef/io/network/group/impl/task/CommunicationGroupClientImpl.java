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
package org.apache.reef.io.network.group.impl.task;

import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.GroupCommOperator;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.group.api.GroupChanges;
import org.apache.reef.io.network.group.api.task.CommGroupNetworkHandler;
import org.apache.reef.io.network.group.api.task.CommunicationGroupServiceClient;
import org.apache.reef.io.network.group.api.task.GroupCommNetworkHandler;
import org.apache.reef.io.network.group.impl.GroupChangesCodec;
import org.apache.reef.io.network.group.impl.GroupChangesImpl;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.parameters.CommunicationGroupName;
import org.apache.reef.io.network.group.impl.config.parameters.OperatorName;
import org.apache.reef.io.network.group.impl.config.parameters.SerializedOperConfigs;
import org.apache.reef.io.network.group.impl.operators.Sender;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class CommunicationGroupClientImpl implements CommunicationGroupServiceClient {
  private static final Logger LOG = Logger.getLogger(CommunicationGroupClientImpl.class.getName());

  private final GroupCommNetworkHandler groupCommNetworkHandler;
  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, GroupCommOperator> operators;
  private final Sender sender;

  private final String taskId;

  private final String driverId;

  private final CommGroupNetworkHandler commGroupNetworkHandler;

  private final AtomicBoolean init = new AtomicBoolean(false);

  @Inject
  public CommunicationGroupClientImpl(@Parameter(CommunicationGroupName.class) final String groupName,
                                      @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                                      @Parameter(DriverIdentifier.class) final String driverId,
                                      final GroupCommNetworkHandler groupCommNetworkHandler,
                                      @Parameter(SerializedOperConfigs.class) final Set<String> operatorConfigs,
                                      final ConfigurationSerializer configSerializer,
                                      final NetworkService<GroupCommunicationMessage> netService) {
    this.taskId = taskId;
    this.driverId = driverId;
    LOG.finest(groupName + " has GroupCommHandler-" + groupCommNetworkHandler.toString());
    this.groupName = Utils.getClass(groupName);
    this.groupCommNetworkHandler = groupCommNetworkHandler;
    this.sender = new Sender(netService);
    this.operators = new TreeMap<>(new Comparator<Class<? extends Name<String>>>() {

      @Override
      public int compare(final Class<? extends Name<String>> o1, final Class<? extends Name<String>> o2) {
        final String s1 = o1.getSimpleName();
        final String s2 = o2.getSimpleName();
        return s1.compareTo(s2);
      }
    });
    try {
      this.commGroupNetworkHandler = Tang.Factory.getTang().newInjector().getInstance(CommGroupNetworkHandler.class);
      this.groupCommNetworkHandler.register(this.groupName, commGroupNetworkHandler);

      for (final String operatorConfigStr : operatorConfigs) {

        final Configuration operatorConfig = configSerializer.fromString(operatorConfigStr);
        final Injector injector = Tang.Factory.getTang().newInjector(operatorConfig);

        injector.bindVolatileParameter(TaskConfigurationOptions.Identifier.class, taskId);
        injector.bindVolatileParameter(CommunicationGroupName.class, groupName);
        injector.bindVolatileInstance(CommGroupNetworkHandler.class, commGroupNetworkHandler);
        injector.bindVolatileInstance(NetworkService.class, netService);
        injector.bindVolatileInstance(CommunicationGroupServiceClient.class, this);

        final GroupCommOperator operator = injector.getInstance(GroupCommOperator.class);
        final String operName = injector.getNamedInstance(OperatorName.class);
        this.operators.put(Utils.getClass(operName), operator);
        LOG.finest(operName + " has CommGroupHandler-" + commGroupNetworkHandler.toString());
      }
    } catch (final InjectionException | IOException e) {
      throw new RuntimeException("Unable to deserialize operator config", e);
    }
  }

  @Override
  public Broadcast.Sender getBroadcastSender(final Class<? extends Name<String>> operatorName) {
    LOG.entering("CommunicationGroupClientImpl", "getBroadcastSender", new Object[]{getQualifiedName(),
        Utils.simpleName(operatorName)});
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Broadcast.Sender)) {
      throw new RuntimeException("Configured operator is not a broadcast sender");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    LOG.exiting("CommunicationGroupClientImpl", "getBroadcastSender", getQualifiedName() + op);
    return (Broadcast.Sender) op;
  }

  @Override
  public Reduce.Receiver getReduceReceiver(final Class<? extends Name<String>> operatorName) {
    LOG.entering("CommunicationGroupClientImpl", "getReduceReceiver", new Object[]{getQualifiedName(),
        Utils.simpleName(operatorName)});
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Reduce.Receiver)) {
      throw new RuntimeException("Configured operator is not a reduce receiver");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    LOG.exiting("CommunicationGroupClientImpl", "getReduceReceiver", getQualifiedName() + op);
    return (Reduce.Receiver) op;
  }

  @Override
  public Broadcast.Receiver getBroadcastReceiver(final Class<? extends Name<String>> operatorName) {
    LOG.entering("CommunicationGroupClientImpl", "getBroadcastReceiver", new Object[]{getQualifiedName(),
        Utils.simpleName(operatorName)});
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Broadcast.Receiver)) {
      throw new RuntimeException("Configured operator is not a broadcast receiver");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    LOG.exiting("CommunicationGroupClientImpl", "getBroadcastReceiver", getQualifiedName() + op);
    return (Broadcast.Receiver) op;
  }

  @Override
  public Reduce.Sender getReduceSender(final Class<? extends Name<String>> operatorName) {
    LOG.entering("CommunicationGroupClientImpl", "getReduceSender", new Object[]{getQualifiedName(),
        Utils.simpleName(operatorName)});
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Reduce.Sender)) {
      throw new RuntimeException("Configured operator is not a reduce sender");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    LOG.exiting("CommunicationGroupClientImpl", "getReduceSender", getQualifiedName() + op);
    return (Reduce.Sender) op;
  }

  @Override
  public void initialize() {
    LOG.entering("CommunicationGroupClientImpl", "initialize", getQualifiedName());
    if (init.compareAndSet(false, true)) {
      LOG.finest("CommGroup-" + groupName + " is initializing");
      final CountDownLatch initLatch = new CountDownLatch(operators.size());

      final InitHandler initHandler = new InitHandler(initLatch);
      final EStage<GroupCommOperator> initStage = new ThreadPoolStage<>(initHandler, operators.size());
      for (final GroupCommOperator op : operators.values()) {
        initStage.onNext(op);
      }

      try {
        initLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException("InterruptedException while waiting for initialization", e);
      }

      if (initHandler.getException() != null) {
        throw new RuntimeException(getQualifiedName() + "Parent dead. Current behavior is for the child to die too.");
      }
    }
    LOG.exiting("CommunicationGroupClientImpl", "initialize", getQualifiedName());
  }

  @Override
  public GroupChanges getTopologyChanges() {
    LOG.entering("CommunicationGroupClientImpl", "getTopologyChanges", getQualifiedName());
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      LOG.finest("Sending TopologyChanges msg to driver");
      try {
        sender.send(Utils.bldVersionedGCM(groupName, operName,
            ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologyChanges, taskId, op.getVersion(), driverId,
            0, Utils.EMPTY_BYTE_ARR));
      } catch (final NetworkException e) {
        throw new RuntimeException("NetworkException while sending GetTopologyChanges", e);
      }
    }
    final Codec<GroupChanges> changesCodec = new GroupChangesCodec();
    final Map<Class<? extends Name<String>>, GroupChanges> perOpChanges = new HashMap<>();
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      final byte[] changes = commGroupNetworkHandler.waitForTopologyChanges(operName);
      perOpChanges.put(operName, changesCodec.decode(changes));
    }
    final GroupChanges retVal = mergeGroupChanges(perOpChanges);
    LOG.exiting("CommunicationGroupClientImpl", "getTopologyChanges", getQualifiedName() + retVal);
    return retVal;
  }

  /**
   * @param perOpChanges
   * @return
   */
  private GroupChanges mergeGroupChanges(final Map<Class<? extends Name<String>>, GroupChanges> perOpChanges) {
    LOG.entering("CommunicationGroupClientImpl", "mergeGroupChanges", new Object[]{getQualifiedName(), perOpChanges});
    boolean doChangesExist = false;
    for (final GroupChanges change : perOpChanges.values()) {
      if (change.exist()) {
        doChangesExist = true;
        break;
      }
    }
    final GroupChanges changes = new GroupChangesImpl(doChangesExist);
    LOG.exiting("CommunicationGroupClientImpl", "mergeGroupChanges", getQualifiedName() + changes);
    return changes;
  }

  @Override
  public void updateTopology() {
    LOG.entering("CommunicationGroupClientImpl", "updateTopology", getQualifiedName());
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      try {
        sender.send(Utils.bldVersionedGCM(groupName, operName,
            ReefNetworkGroupCommProtos.GroupCommMessage.Type.UpdateTopology, taskId, op.getVersion(), driverId,
            0, Utils.EMPTY_BYTE_ARR));
      } catch (final NetworkException e) {
        throw new RuntimeException("NetworkException while sending UpdateTopology", e);
      }
    }
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      GroupCommunicationMessage msg;
      do {
        msg = commGroupNetworkHandler.waitForTopologyUpdate(operName);
      } while (!isMsgVersionOk(msg));
    }
    LOG.exiting("CommunicationGroupClientImpl", "updateTopology", getQualifiedName());
  }

  private boolean isMsgVersionOk(final GroupCommunicationMessage msg) {
    LOG.entering("CommunicationGroupClientImpl", "isMsgVersionOk", new Object[]{getQualifiedName(), msg});
    if (msg.hasVersion()) {
      final int msgVersion = msg.getVersion();
      final GroupCommOperator operator = operators.get(Utils.getClass(msg.getOperatorname()));
      final int nodeVersion = operator.getVersion();
      final boolean retVal;
      if (msgVersion < nodeVersion) {
        LOG.warning(getQualifiedName() + "Received a ver-" + msgVersion + " msg while expecting ver-" + nodeVersion
            + ". Discarding msg");
        retVal = false;
      } else {
        retVal = true;
      }
      LOG.exiting("CommunicationGroupClientImpl", "isMsgVersionOk",
          Arrays.toString(new Object[]{retVal, getQualifiedName(), msg}));
      return retVal;
    } else {
      throw new RuntimeException(getQualifiedName() + "can only deal with versioned msgs");
    }
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + " ";
  }

  @Override
  public Class<? extends Name<String>> getName() {
    return groupName;
  }

}
