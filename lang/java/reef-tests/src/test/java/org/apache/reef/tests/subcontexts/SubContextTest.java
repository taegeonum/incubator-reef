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
package org.apache.reef.tests.subcontexts;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.library.driver.OnDriverStartedAllocateOne;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the creation of context stacks on an Evaluator.
 */
public final class SubContextTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testSubContexts() throws BindException, InjectionException {

    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_SubContextTest_testSubContexts")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SubContextDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SubContextDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, SubContextDriver.ContextClosedHandler.class)
        .build();

    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfiguration, this.testEnvironment.getTestTimeout());

    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }

  /**
   * Same as testSubContexts(), but using default ClosedContext handler.
   */
  @Test
  public void testChainClose() throws BindException, InjectionException {

    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_SubContextTest_testChainClose")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SubContextDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SubContextDriver.ContextActiveHandler.class)
        .build();

    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfiguration, this.testEnvironment.getTestTimeout());

    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }
}
