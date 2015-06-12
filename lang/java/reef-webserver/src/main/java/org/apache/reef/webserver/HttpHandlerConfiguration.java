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
package org.apache.reef.webserver;

import org.apache.reef.runtime.yarn.driver.TrackingURLProvider;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * Configuration Module Builder for Http Handler.
 */
public final class HttpHandlerConfiguration extends ConfigurationModuleBuilder {

  /**
   * Specify optional parameter for HttpEventHandlers.
   */
  public static final OptionalParameter<HttpHandler> HTTP_HANDLERS = new OptionalParameter<>();

  /**
   * HttpHandlerConfiguration merged with HttpRuntimeConfiguration.
   */
  public static final ConfigurationModule CONF = new HttpHandlerConfiguration().merge(HttpRuntimeConfiguration.CONF)
      .bindSetEntry(HttpEventHandlers.class, HTTP_HANDLERS)
      .bindImplementation(TrackingURLProvider.class, HttpTrackingURLProvider.class)
      .build();
}
