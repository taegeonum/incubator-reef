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
package org.apache.reef.io.network.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

public class UnbindNSFromTask implements EventHandler<TaskStop> {

  private final NetworkService<?> ns;
  private final IdentifierFactory idFac;

  @Inject
  public UnbindNSFromTask(
      final NetworkService<?> ns,
      @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory idFac) {
    this.ns = ns;
    this.idFac = idFac;
  }

  @Override
  public void onNext(final TaskStop task) {
    this.ns.unregisterId(this.idFac.getNewInstance(task.getId()));
  }
}
