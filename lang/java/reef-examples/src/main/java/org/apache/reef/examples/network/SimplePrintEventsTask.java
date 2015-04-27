/**
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
package org.apache.reef.examples.network;

import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * SimplePrintEventsTask
 */
public final class SimplePrintEventsTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimplePrintEventsTask.class.getName());

  private final FirstEventHandler firstEventHandler;
  private final SecondEventHandler secondEventHandler;

  @Inject
  public SimplePrintEventsTask(
          final FirstEventHandler firstEventHandler,
          final SecondEventHandler secondEventHandler) {
    this.firstEventHandler = firstEventHandler;
    this.secondEventHandler = secondEventHandler;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    FirstEvent firstEvent = firstEventHandler.getEvent();
    System.out.println("Receive FirstEvent!");
    LOG.log(Level.INFO, "Receive FirstEvent!");

    SecondEvent secondEvent = secondEventHandler.getEvent();
    System.out.println("Receive SecondEvent!");
    LOG.log(Level.INFO, "Receive SecondEvent!");
    return null;
  }
}
