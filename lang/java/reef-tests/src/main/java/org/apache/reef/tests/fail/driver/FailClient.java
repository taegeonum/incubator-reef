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
package org.apache.reef.tests.fail.driver;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestDriverLauncher;
import org.apache.reef.util.EnvironmentUtils;

/**
 * Client for the test REEF job that fails on different stages of execution.
 */
public final class FailClient {

  public static LauncherStatus run(final Class<?> failMsgClass,
                                   final Configuration runtimeConfig,
                                   final int timeOut) throws BindException, InjectionException {

    final Configuration driverConfig = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(FailDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "Fail_" + failMsgClass.getSimpleName())
        .set(DriverConfiguration.ON_DRIVER_STARTED, FailDriver.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, FailDriver.StopHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, FailDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, FailDriver.CompletedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, FailDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, FailDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_MESSAGE, FailDriver.ContextMessageHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, FailDriver.ClosedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, FailDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, FailDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_SUSPENDED, FailDriver.SuspendedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, FailDriver.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, FailDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, FailDriver.CompletedTaskHandler.class)
        .build();

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.addConfiguration(driverConfig);
    cb.bindNamedParameter(FailDriver.FailMsgClassName.class, failMsgClass.getName());

    return TestDriverLauncher.getLauncher(runtimeConfig).run(cb.build(), timeOut);
  }
}
