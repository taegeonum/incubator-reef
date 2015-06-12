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
package org.apache.reef.runtime.local.driver;

import org.apache.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseHandler;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.LocalClasspathProvider;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * ConfigurationModule for the Driver executed in the local resourcemanager. This is meant to eventually replace
 * LocalDriverRuntimeConfiguration.
 */
public class LocalDriverConfiguration extends ConfigurationModuleBuilder {
  /**
   * The maximum number of evaluators.
   */
  public static final RequiredParameter<Integer> MAX_NUMBER_OF_EVALUATORS = new RequiredParameter<>();
  /**
   * The root folder of the job. Assumed to be an absolute path.
   */
  public static final RequiredParameter<String> ROOT_FOLDER = new RequiredParameter<>();
  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  /**
   * The remote identifier to use for communications back to the client.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

  /**
   * The identifier of the Job submitted.
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new LocalDriverConfiguration()
      .bindImplementation(ResourceLaunchHandler.class, LocalResourceLaunchHandler.class)
      .bindImplementation(ResourceRequestHandler.class, LocalResourceRequestHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, LocalResourceReleaseHandler.class)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(MaxNumberOfEvaluators.class, MAX_NUMBER_OF_EVALUATORS)
      .bindNamedParameter(RootFolder.class, ROOT_FOLDER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
      .build();
}
