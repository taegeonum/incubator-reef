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
package org.apache.reef.client;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.runtime.common.client.RunningJobImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Represents a running REEF job.
 */
@Public
@ClientSide
@Provided
@DefaultImplementation(RunningJobImpl.class)
public interface RunningJob extends Identifiable, AutoCloseable {

  /**
   * Cancels the running Job.
   */
  @Override
  public void close();

  /**
   * Cancels the running Job.
   *
   * @param message delivered along with cancel request.
   */
  public void close(final byte[] message);

  /**
   * @return the ID of the running job.
   */
  @Override
  public String getId();

  /**
   * Send a message to the Driver.
   *
   * @param message to send to the running driver
   */
  public void send(final byte[] message);
}
