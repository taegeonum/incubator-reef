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
package org.apache.reef.runtime.yarn.driver;

import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseHandler;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ResourceReleaseHandler for YARN.
 */
public final class YARNResourceReleaseHandler implements ResourceReleaseHandler {

  private static final Logger LOG = Logger.getLogger(YARNResourceReleaseHandler.class.getName());

  private final InjectionFuture<YarnContainerManager> yarnContainerManager;

  @Inject
  YARNResourceReleaseHandler(final InjectionFuture<YarnContainerManager> yarnContainerManager) {
    this.yarnContainerManager = yarnContainerManager;
    LOG.log(Level.FINE, "Instantiated 'YARNResourceReleaseHandler'");
  }

  @Override
  public void onNext(final ResourceReleaseEvent resourceReleaseEvent) {
    final String containerId = resourceReleaseEvent.getIdentifier();
    LOG.log(Level.FINEST, "Releasing container {0}", containerId);
    this.yarnContainerManager.get().release(containerId);
  }
}
