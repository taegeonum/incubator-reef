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
package org.apache.reef.runtime.mesos.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.mesos.client.parameters.MasterIp;
import org.apache.reef.runtime.mesos.client.parameters.RootFolder;
import org.apache.reef.runtime.mesos.driver.MesosDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The current implementation runs the driver as a local process, similar to reef-runtime-local.
 * TODO: run the driver on a slave node in the cluster
 */
@Private
@ClientSide
final class MesosJobSubmissionHandler implements JobSubmissionHandler {
  public static final String DRIVER_FOLDER_NAME = "driver";

  private final ConfigurationSerializer configurationSerializer;
  private final ClasspathProvider classpath;
  private final REEFFileNames fileNames;
  private final String rootFolderName;
  private final String masterIp;
  private final double jvmSlack;

  @Inject
  MesosJobSubmissionHandler(@Parameter(RootFolder.class) final String rootFolderName,
                            @Parameter(MasterIp.class) final String masterIp,
                            final ConfigurationSerializer configurationSerializer,
                            final REEFFileNames fileNames,
                            final ClasspathProvider classpath,
                            @Parameter(JVMHeapSlack.class) final double jvmSlack) {
    this.rootFolderName = new File(rootFolderName).getAbsolutePath();
    this.masterIp = masterIp;
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.classpath = classpath;
    this.jvmSlack = jvmSlack;
  }

  @Override
  public void close() {
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {
    try {
      final File jobFolder = new File(new File(this.rootFolderName),
          "/" + jobSubmissionEvent.getIdentifier() + "-" + System.currentTimeMillis() + "/");

      final File driverFolder = new File(jobFolder, DRIVER_FOLDER_NAME);
      driverFolder.mkdirs();

      final File reefFolder = new File(driverFolder, this.fileNames.getREEFFolderName());
      reefFolder.mkdirs();

      final File localFolder = new File(reefFolder, this.fileNames.getLocalFolderName());
      localFolder.mkdirs();
      for (final FileResource file : jobSubmissionEvent.getLocalFileSet()) {
        final Path src = new File(file.getPath()).toPath();
        final Path dst = new File(driverFolder, this.fileNames.getLocalFolderPath() + "/" + file.getName()).toPath();
        Files.copy(src, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }

      final File globalFolder = new File(reefFolder, this.fileNames.getGlobalFolderName());
      globalFolder.mkdirs();
      for (final FileResource file : jobSubmissionEvent.getGlobalFileSet()) {
        final Path src = new File(file.getPath()).toPath();
        final Path dst = new File(driverFolder, this.fileNames.getGlobalFolderPath() + "/" + file.getName()).toPath();
        Files.copy(src, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }

      final Configuration driverConfiguration =
          Configurations.merge(MesosDriverConfiguration.CONF
              .set(MesosDriverConfiguration.MESOS_MASTER_IP, this.masterIp)
              .set(MesosDriverConfiguration.JOB_IDENTIFIER, jobSubmissionEvent.getIdentifier())
              .set(MesosDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, jobSubmissionEvent.getRemoteId())
              .set(MesosDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
              .set(MesosDriverConfiguration.SCHEDULER_DRIVER_CAPACITY, 1)
              // must be 1 as there is 1 scheduler at the same time
              .build(),
          jobSubmissionEvent.getConfiguration());
      final File runtimeConfigurationFile = new File(driverFolder, this.fileNames.getDriverConfigurationPath());
      this.configurationSerializer.toFile(driverConfiguration, runtimeConfigurationFile);

      final List<String> launchCommand = new JavaLaunchCommandBuilder()
          .setErrorHandlerRID(jobSubmissionEvent.getRemoteId())
          .setLaunchID(jobSubmissionEvent.getIdentifier())
          .setConfigurationFileName(this.fileNames.getDriverConfigurationPath())
          .setClassPath(this.classpath.getDriverClasspath())
          .setMemory(jobSubmissionEvent.getDriverMemory().get())
          .build();

      final File errFile = new File(driverFolder, fileNames.getDriverStderrFileName());
      final File outFile = new File(driverFolder, fileNames.getDriverStdoutFileName());

      new ProcessBuilder()
          .command(launchCommand)
          .directory(driverFolder)
          .redirectError(errFile)
          .redirectOutput(outFile)
          .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
