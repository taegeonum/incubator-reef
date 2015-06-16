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
package org.apache.reef.runtime.common.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.remote.RemoteMessage;

/**
 * Manages the RunningJobs a client knows about.
 */
@Private
@ClientSide
@DefaultImplementation(RunningJobsImpl.class)
interface RunningJobs {

  /**
   * Closes all registered jobs forcefully.
   */
  public void closeAllJobs();

  /**
   * Processes a status message from a Job. If the Job is already known, the message will be passed on. If it is a
   * first message, a new RunningJob instance will be created for it.
   *
   * @param message
   */
  public void onJobStatusMessage(final RemoteMessage<ReefServiceProtos.JobStatusProto> message);

  /**
   * Processes a error message from the resource manager.
   *
   * @param runtimeFailure
   */
  public void onRuntimeErrorMessage(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> runtimeFailure);

}
