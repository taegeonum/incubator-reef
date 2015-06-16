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
package org.apache.reef.tests.roguethread;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.library.driver.ExpectedTaskFailureHandler;
import org.apache.reef.tests.library.driver.OnDriverStartedAllocateOne;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests whether an exception thrown by a thread spawned by Task.call() triggers a FailedTask in the Driver.
 */
public final class RogueThreadTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testRogueThread() throws InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "Test_RogueThreadTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, RogueThreadDriver.EvaluatorAllocationHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, ExpectedTaskFailureHandler.class)
        .build();
    final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfiguration, this.testEnvironment.getTestTimeout());

    Assert.assertTrue("Job state after execution: " + state, state.isSuccess());


  }
}
