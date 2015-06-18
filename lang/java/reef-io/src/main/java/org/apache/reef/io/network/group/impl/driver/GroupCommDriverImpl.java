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

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.NetworkServiceConfigurationBuilder;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommServiceDriver;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessageCodec;
import org.apache.reef.io.network.group.impl.config.parameters.GroupCommServiceId;
import org.apache.reef.io.network.group.impl.config.parameters.SerializedGroupConfigs;
import org.apache.reef.io.network.group.impl.config.parameters.TreeTopologyFanOut;
import org.apache.reef.io.network.group.impl.task.GroupCommNetworkHandlerImpl;
import org.apache.reef.io.network.group.impl.utils.BroadcastingEventHandler;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.SingletonAsserter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Sets up various stages to handle REEF events and adds the per communication
 * group stages to them whenever a new communication group is created.
 * <p/>
 * Also starts the NameService & the NetworkService on the driver
 */
public class GroupCommDriverImpl implements GroupCommServiceDriver {
  private static final Logger LOG = Logger.getLogger(GroupCommDriverImpl.class.getName());
  /**
   * TANG instance.
   */
  private static final Tang tang = Tang.Factory.getTang();

  private final AtomicInteger contextIds = new AtomicInteger(0);

  private final IdentifierFactory idFac = new StringIdentifierFactory();

  private final NameServer nameService;

  private final String nameServiceAddr;
  private final int nameServicePort;

  private final Map<Class<? extends Name<String>>, CommunicationGroupDriver> commGroupDrivers = new HashMap<>();

  private final ConfigurationSerializer confSerializer;

  private final ConnectionFactory<GroupCommunicationMessage> connFactory;

  private final EStage<GroupCommunicationMessage> senderStage;

  private final String driverId;
  private final BroadcastingEventHandler<RunningTask> groupCommRunningTaskHandler;
  private final EStage<RunningTask> groupCommRunningTaskStage;
  private final BroadcastingEventHandler<FailedTask> groupCommFailedTaskHandler;
  private final EStage<FailedTask> groupCommFailedTaskStage;
  private final BroadcastingEventHandler<FailedEvaluator> groupCommFailedEvaluatorHandler;
  private final EStage<FailedEvaluator> groupCommFailedEvaluatorStage;
  private final GroupCommMessageHandler groupCommMessageHandler;
  private final int fanOut;
  private final NetworkServiceConfigurationBuilder netServiceBuilder;

  /**
   * @deprecated Have an instance injected instead.
   */
  @Deprecated
  @Inject
  public GroupCommDriverImpl(final ConfigurationSerializer confSerializer,
                             @Parameter(DriverIdentifier.class) final String driverId,
                             @Parameter(TreeTopologyFanOut.class) final int fanOut,
                             final NetworkServiceConfigurationBuilder netServiceBuilder,
                             final NetworkService netService) throws NetworkException {
    this(confSerializer, driverId, fanOut, netServiceBuilder, netService, new GroupCommMessageHandler(), LocalAddressProviderFactory.getInstance());
  }

  /**
   * @deprecated Have an instance injected instead.
   */
  @Deprecated
  @Inject
  public GroupCommDriverImpl(final ConfigurationSerializer confSerializer,
                             @Parameter(DriverIdentifier.class) final String driverId,
                             @Parameter(TreeTopologyFanOut.class) final int fanOut,
                             final NetworkServiceConfigurationBuilder netServiceBuilder,
                             final NetworkService netService,
                             final GroupCommMessageHandler groupCommMessageHandler,
                             final LocalAddressProvider localAddressProvider) throws NetworkException {
    assert (SingletonAsserter.assertSingleton(getClass()));
    this.driverId = driverId;
    this.fanOut = fanOut;
    this.nameService = new NameServerImpl(0, idFac, localAddressProvider);
    this.nameServiceAddr = localAddressProvider.getLocalAddress();
    this.nameServicePort = nameService.getPort();
    this.confSerializer = confSerializer;
    this.groupCommRunningTaskHandler = new BroadcastingEventHandler<>();
    this.groupCommRunningTaskStage = new SyncStage<>("GroupCommRunningTaskStage", groupCommRunningTaskHandler);
    this.groupCommFailedTaskHandler = new BroadcastingEventHandler<>();
    this.groupCommFailedTaskStage = new SyncStage<>("GroupCommFailedTaskStage", groupCommFailedTaskHandler);
    this.groupCommFailedEvaluatorHandler = new BroadcastingEventHandler<>();
    this.groupCommFailedEvaluatorStage = new SyncStage<>("GroupCommFailedEvaluatorStage",
        groupCommFailedEvaluatorHandler);
    this.groupCommMessageHandler = groupCommMessageHandler;
    netService.registerId(idFac.getNewInstance(driverId));
    this.connFactory = netService.newConnectionFactory(GroupCommServiceId.class);
    this.netServiceBuilder = netServiceBuilder;
    this.senderStage = new ThreadPoolStage<>("SrcCtrlMsgSender", new CtrlMsgSender(idFac, netService), 5);
  }

  @Override
  public CommunicationGroupDriver newCommunicationGroup(final Class<? extends Name<String>> groupName,
                                                        final int numberOfTasks) {
    LOG.entering("GroupCommDriverImpl", "newCommunicationGroup", new Object[]{Utils.simpleName(groupName), numberOfTasks});
    final BroadcastingEventHandler<RunningTask> commGroupRunningTaskHandler = new BroadcastingEventHandler<>();
    final BroadcastingEventHandler<FailedTask> commGroupFailedTaskHandler = new BroadcastingEventHandler<>();
    final BroadcastingEventHandler<FailedEvaluator> commGroupFailedEvaluatorHandler = new BroadcastingEventHandler<>();
    final BroadcastingEventHandler<GroupCommunicationMessage> commGroupMessageHandler = new BroadcastingEventHandler<>();
    final CommunicationGroupDriver commGroupDriver = new CommunicationGroupDriverImpl(groupName, confSerializer,
        senderStage,
        commGroupRunningTaskHandler,
        commGroupFailedTaskHandler,
        commGroupFailedEvaluatorHandler,
        commGroupMessageHandler,
        driverId, numberOfTasks, fanOut);
    commGroupDrivers.put(groupName, commGroupDriver);
    groupCommRunningTaskHandler.addHandler(commGroupRunningTaskHandler);
    groupCommFailedTaskHandler.addHandler(commGroupFailedTaskHandler);
    groupCommMessageHandler.addHandler(groupName, commGroupMessageHandler);
    LOG.exiting("GroupCommDriverImpl", "newCommunicationGroup", "Created communication group: " + Utils.simpleName(groupName));
    return commGroupDriver;
  }

  @Override
  public boolean isConfigured(final ActiveContext activeContext) {
    LOG.entering("GroupCommDriverImpl", "isConfigured", activeContext.getId());
    final boolean retVal = activeContext.getId().startsWith("GroupCommunicationContext-");
    LOG.exiting("GroupCommDriverImpl", "isConfigured", retVal);
    return retVal;
  }

  @Override
  public Configuration getContextConfiguration() {
    LOG.entering("GroupCommDriverImpl", "getContextConf");
    final Configuration retVal = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
        "GroupCommunicationContext-" + contextIds.getAndIncrement()).build();
    LOG.exiting("GroupCommDriverImpl", "getContextConf", confSerializer.toString(retVal));
    return retVal;
  }

  @Override
  public Configuration getServiceConfiguration(final String contextId) {
    LOG.entering("GroupCommDriverImpl", "getServiceConf");
    netServiceBuilder.setConnectionFactory(contextId, GroupCommServiceId.class, GroupCommunicationMessageCodec.class,
        GroupCommNetworkHandlerImpl.class, GroupCommLinkListener.class);

    final Configuration serviceConfiguration = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES,
            GroupCommNetworkHandlerImpl.class).build();

    final Configuration retVal = tang.newConfigurationBuilder(serviceConfiguration,
        netServiceBuilder.getServiceConfiguration(contextId, GroupCommServiceId.class))
        .build();
    LOG.exiting("GroupCommDriverImpl", "getServiceConf", confSerializer.toString(retVal));
    return retVal;
  }

  @Override
  public Configuration getTaskConfiguration(final Configuration partialTaskConf) {
    LOG.entering("GroupCommDriverImpl", "getTaskConfiguration", new Object[]{confSerializer.toString(partialTaskConf)});
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf);
    for (final CommunicationGroupDriver commGroupDriver : commGroupDrivers.values()) {
      final Configuration commGroupConf = commGroupDriver.getTaskConfiguration(partialTaskConf);
      if (commGroupConf != null) {
        jcb.bindSetEntry(SerializedGroupConfigs.class, confSerializer.toString(commGroupConf));
      }
    }
    final Configuration retVal = jcb.build();
    LOG.exiting("GroupCommDriverImpl", "getTaskConfiguration", confSerializer.toString(retVal));
    return retVal;
  }

  /**
   * @return the groupCommRunningTaskStage
   */
  @Override
  public EStage<RunningTask> getGroupCommRunningTaskStage() {
    LOG.entering("GroupCommDriverImpl", "getGroupCommRunningTaskStage");
    LOG.exiting("GroupCommDriverImpl", "getGroupCommRunningTaskStage", "Returning GroupCommRunningTaskStage");
    return groupCommRunningTaskStage;
  }

  /**
   * @return the groupCommFailedTaskStage
   */
  @Override
  public EStage<FailedTask> getGroupCommFailedTaskStage() {
    LOG.entering("GroupCommDriverImpl", "getGroupCommFailedTaskStage");
    LOG.exiting("GroupCommDriverImpl", "getGroupCommFailedTaskStage", "Returning GroupCommFailedTaskStage");
    return groupCommFailedTaskStage;
  }

  /**
   * @return the groupCommFailedEvaluatorStage
   */
  @Override
  public EStage<FailedEvaluator> getGroupCommFailedEvaluatorStage() {
    LOG.entering("GroupCommDriverImpl", "getGroupCommFailedEvaluatorStage");
    LOG.exiting("GroupCommDriverImpl", "getGroupCommFailedEvaluatorStage", "Returning GroupCommFaileEvaluatorStage");
    return groupCommFailedEvaluatorStage;
  }

}
