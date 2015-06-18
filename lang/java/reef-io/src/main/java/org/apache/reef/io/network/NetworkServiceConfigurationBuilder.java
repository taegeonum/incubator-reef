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

package org.apache.reef.io.network;

import org.apache.reef.io.network.config.NetworkServiceConfigurationBuilderImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

/*
 * Network Service configuration for services (tasks)
 */
@DefaultImplementation(NetworkServiceConfigurationBuilderImpl.class)
public interface NetworkServiceConfigurationBuilder {

  /**
   * Sets configurations of ConnectionFactory for EvaluatorSide NetworkService.
   * @param contextId
   * @param connectionFactoryId
   * @param codec
   * @param eventHandler
   * @param linkListener
   */
  public void setConnectionFactory(String contextId,
                                   Class<? extends Name<String>> connectionFactoryId,
                                   Class<? extends Codec<?>> codec,
                                   Class<? extends EventHandler<?>> eventHandler,
                                   Class<? extends LinkListener<?>> linkListener);

  /**
   * Sets configurations of ConnectionFactory for EvaluatorSide NetworkService.
   * @param contextId
   * @param connectionFactoryId
   * @param codec
   * @param eventHandler
   */
  public void setConnectionFactory(String contextId,
                                   Class<? extends Name<String>> connectionFactoryId,
                                   Class<? extends Codec<?>> codec,
                                   Class<? extends EventHandler<?>> eventHandler);


  /**
   * Gets a service configuration corresponding to contextId and connectionFactoryId.
   * @param contextId
   * @param connectionFactoryId
   */
  public Configuration getServiceConfiguration(String contextId,
                                               Class<? extends Name<String>> connectionFactoryId);

}
