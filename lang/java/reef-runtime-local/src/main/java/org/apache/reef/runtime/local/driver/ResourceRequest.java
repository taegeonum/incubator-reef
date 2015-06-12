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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;

/**
 * Manages a ResourceRequestEvent and its satisfaction.
 */
@Private
@DriverSide
final class ResourceRequest {

  private final ResourceRequestEvent req;
  private int satisfied = 0;

  ResourceRequest(final ResourceRequestEvent req) {
    if (null == req) {
      throw new IllegalArgumentException("Can't instantiate a ResourceRequest without a ResourceRequestEvent");
    }
    this.req = req;
  }

  /**
   * Records that one resource has been allocated to this Request.
   */
  final void satisfyOne() {
    this.satisfied += 1;
    if (this.satisfied > req.getResourceCount()) {
      throw new IllegalStateException("This request has been oversatisfied.");
    }
  }

  /**
   * @return true if the request is satisfied with this additional unit of
   * resource, false otherwise.
   */
  final boolean isSatisfied() {
    return this.satisfied == req.getResourceCount();
  }

  final ResourceRequestEvent getRequestProto() {
    return this.req;
  }
}
