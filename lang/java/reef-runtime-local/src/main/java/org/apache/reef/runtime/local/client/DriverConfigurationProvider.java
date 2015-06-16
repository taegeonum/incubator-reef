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
package org.apache.reef.runtime.local.client;

import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;

/**
 * Helper class that assembles the driver configuration when run on the local runtime.
 */
public final class DriverConfigurationProvider {

  private final int maxEvaluators;
  private final double jvmHeapSlack;

  @Inject
  DriverConfigurationProvider(final @Parameter(MaxNumberOfEvaluators.class) int maxEvaluators,
                              final @Parameter(JVMHeapSlack.class) double jvmHeapSlack) {
    this.maxEvaluators = maxEvaluators;
    this.jvmHeapSlack = jvmHeapSlack;
  }

  private Configuration getDriverConfiguration(final File jobFolder,
                                               final String clientRemoteId,
                                               final String jobId) {
    return LocalDriverConfiguration.CONF
        .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, this.maxEvaluators)
        .set(LocalDriverConfiguration.ROOT_FOLDER, jobFolder.getAbsolutePath())
        .set(LocalDriverConfiguration.JVM_HEAP_SLACK, this.jvmHeapSlack)
        .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
        .set(LocalDriverConfiguration.JOB_IDENTIFIER, jobId)
        .build();
  }

  /**
   * Assembles the driver configuration.
   *
   * @param jobFolder                The folder in which the local runtime will execute this job.
   * @param clientRemoteId           the remote identifier of the client. It is used by the Driver to establish a
   *                                 connection back to the client.
   * @param jobId                    The identifier of the job.
   * @param applicationConfiguration The configuration of the application, e.g. a filled out DriverConfiguration
   * @return The Driver configuration to be used to instantiate the Driver.
   */
  public Configuration getDriverConfiguration(final File jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    return Configurations.merge(getDriverConfiguration(jobFolder, clientRemoteId, jobId), applicationConfiguration);
  }
}
