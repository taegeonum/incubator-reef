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
package org.apache.reef.tests;

import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;

/**
 * A TestEnvironment for the local resourcemanager.
 */
public final class LocalTestEnvironment extends TestEnvironmentBase implements TestEnvironment {

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently
   */
  public static final int MAX_NUMBER_OF_EVALUATORS = 4;
  // Used to make sure the tests call the methods in the right order.
  private boolean ready = false;

  @Override
  public synchronized final void setUp() {
    this.ready = true;
  }

  @Override
  public synchronized final Configuration getRuntimeConfiguration() {
    assert (this.ready);
    final String rootFolder = System.getProperty("org.apache.reef.runtime.local.folder");
    if (null == rootFolder) {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, rootFolder)
          .build();

    }
  }

  @Override
  public synchronized final void tearDown() {
    assert (this.ready);
    this.ready = false;
  }

  @Override
  public int getTestTimeout() {
    return 60000; // 1 min.
  }
}
