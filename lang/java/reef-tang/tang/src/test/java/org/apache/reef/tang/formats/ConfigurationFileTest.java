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
package org.apache.reef.tang.formats;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.test.RoundTripTest;

import java.io.File;

/**
 * Tests the file writing routines in ConfigurationFile.
 */
public final class ConfigurationFileTest extends RoundTripTest {
  @Override
  public Configuration roundTrip(final Configuration configuration) throws Exception {
    final File tempFile = java.io.File.createTempFile("TangTest", "txt");
    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    serializer.toTextFile(configuration, tempFile);
    final JavaConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final Configuration conf = serializer.fromTextFile(tempFile);
    configurationBuilder.addConfiguration(conf);
    tempFile.delete();
    return configurationBuilder.build();
  }

  @Override
  public Configuration roundTrip(final Configuration configuration, final ClassHierarchy classHierarchy) throws Exception {
    final File tempFile = java.io.File.createTempFile("TangTest", "txt");
    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    serializer.toTextFile(configuration, tempFile);
    final Configuration conf = serializer.fromTextFile(tempFile, classHierarchy);
    tempFile.delete();
    return conf;
  }
}
