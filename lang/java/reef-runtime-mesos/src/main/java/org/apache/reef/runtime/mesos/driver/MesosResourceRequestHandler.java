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
package org.apache.reef.runtime.mesos.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;

import javax.inject.Inject;

@DriverSide
@Private
final class MesosResourceRequestHandler implements ResourceRequestHandler {
  private final REEFScheduler REEFScheduler;

  @Inject
  MesosResourceRequestHandler(final REEFScheduler REEFScheduler) {
    this.REEFScheduler = REEFScheduler;
  }

  @Override
  public void onNext(final ResourceRequestEvent resourceRequestEvent) {
    REEFScheduler.onResourceRequest(resourceRequestEvent);
  }
}
