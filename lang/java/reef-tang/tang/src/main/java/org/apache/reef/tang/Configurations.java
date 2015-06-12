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
package org.apache.reef.tang;

/**
 * Helper class for Configurations.
 */
public final class Configurations {

  /**
   * This is a utility class that isn't meant to be instantiated.
   */
  private Configurations() {
  }


  /**
   * Merge a set of Configurations.
   *
   * @param configurations
   * @return the merged configuration.
   * @throws org.apache.reef.tang.exceptions.BindException if the merge fails.
   */
  public static Configuration merge(final Configuration... configurations) {
    return Tang.Factory.getTang().newConfigurationBuilder(configurations).build();
  }

  /**
   * Merge a set of Configurations.
   *
   * @param configurations
   * @return the merged configuration.
   * @throws org.apache.reef.tang.exceptions.BindException if the merge fails.
   */
  public static Configuration merge(final Iterable<Configuration> configurations) {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final Configuration configuration : configurations) {
      configurationBuilder.addConfiguration(configuration);
    }
    return configurationBuilder.build();
  }

}
