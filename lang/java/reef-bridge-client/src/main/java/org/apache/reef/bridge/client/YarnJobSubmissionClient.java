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
package org.apache.reef.bridge.client;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.YarnSubmissionHelper;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.JARFileMaker;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java-side of the C# YARN Job Submission API.
 */
@SuppressWarnings("checkstyle:hideutilityclassconstructor")
public final class YarnJobSubmissionClient {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionClient.class.getName());
  private final JobUploader uploader;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames fileNames;
  private final YarnConfiguration yarnConfiguration;
  private final ClasspathProvider classpath;

  @Inject
  YarnJobSubmissionClient(final JobUploader uploader,
                          final YarnConfiguration yarnConfiguration,
                          final ConfigurationSerializer configurationSerializer,
                          final REEFFileNames fileNames,
                          final ClasspathProvider classpath) {
    this.uploader = uploader;
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.yarnConfiguration = yarnConfiguration;
    this.classpath = classpath;
  }

  private void addJVMConfiguration(final File driverFolder, final String jobId, final String jobSubmissionFolder) throws IOException {
    final File driverConfigurationFile = new File(driverFolder, this.fileNames.getDriverConfigurationPath());
    final Configuration driverConfiguration = Configurations.merge(
        Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER,
        YarnDriverConfiguration.CONF
            .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, jobSubmissionFolder)
            .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobId)
            .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
            .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
            .build());

    this.configurationSerializer.toFile(driverConfiguration, driverConfigurationFile);
  }

  /**
   * @param driverFolder the folder containing the `reef` folder. Only that `reef` folder will be in the JAR.
   * @return
   * @throws IOException
   */
  private File makeJar(final File driverFolder) throws IOException {
    final File jarFile = new File(driverFolder.getParentFile(), driverFolder.getName() + ".jar");
    final File reefFolder = new File(driverFolder, fileNames.getREEFFolderName());
    if (!reefFolder.isDirectory()) {
      throw new FileNotFoundException(reefFolder.getAbsolutePath());
    }

    new JARFileMaker(jarFile).addChildren(reefFolder).close();
    return jarFile;
  }

  /**
   * @param driverFolder the folder on the local filesystem that contains the driver's working directory to be
   *                     submitted.
   * @param jobId        the ID of the job
   * @param priority     the priority associated with this Driver
   * @param queue        the queue to submit the driver to
   * @param driverMemory in MB
   * @throws IOException
   * @throws YarnException
   */
  private void launch(final File driverFolder,
                      final String jobId,
                      final int priority,
                      final String queue,
                      final int driverMemory)
      throws IOException, YarnException {
    if (!driverFolder.exists()) {
      throw new IOException("The Driver folder" + driverFolder.getAbsolutePath() + "doesn't exist.");
    }

    // ------------------------------------------------------------------------
    // Get an application ID
    try (final YarnSubmissionHelper submissionHelper =
             new YarnSubmissionHelper(yarnConfiguration, fileNames, classpath)) {


      // ------------------------------------------------------------------------
      // Prepare the JAR
      final JobFolder jobFolderOnDFS = this.uploader.createJobFolder(submissionHelper.getApplicationId());
      this.addJVMConfiguration(driverFolder, jobId, jobFolderOnDFS.getPath().toString());
      final File jarFile = makeJar(driverFolder);
      LOG.log(Level.INFO, "Created job submission jar file: {0}", jarFile);


      // ------------------------------------------------------------------------
      // Upload the JAR
      LOG.info("Uploading job submission JAR");
      final LocalResource jarFileOnDFS = jobFolderOnDFS.uploadAsLocalResource(jarFile);
      LOG.info("Uploaded job submission JAR");


      // ------------------------------------------------------------------------
      // Submit
      submissionHelper
          .addLocalResource(this.fileNames.getREEFFolderName(), jarFileOnDFS)
          .setApplicationName(jobId)
          .setDriverMemory(driverMemory)
          .setPriority(priority)
          .setQueue(queue)
          .submit(ClientRemoteIdentifier.NONE);
    }
  }

  private static Configuration getRuntimeConfiguration(int tcpBeginPort, int tcpRangeCount, int tcpTryCount) {
    Configuration yarnClientConfig = YarnClientConfiguration.CONF
        .build();

    Configuration providerConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(tcpBeginPort))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(tcpRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(tcpTryCount))
        .build();

    return Configurations.merge(yarnClientConfig, providerConfig);
  }

  public static void main(final String[] args) throws InjectionException, IOException, YarnException {
    final File driverFolder = new File(args[0]);
    final String jobId = args[1];
    final int driverMemory = Integer.valueOf(args[2]);
    final int tcpBeginPort = Integer.valueOf(args[3]);
    final int tcpRangeCount = Integer.valueOf(args[4]);
    final int tcpTryCount = Integer.valueOf(args[5]);
    // Static for now
    final int priority = 1;
    final String queue = "default";

    final Configuration yarnConfiguration = getRuntimeConfiguration(tcpBeginPort, tcpRangeCount, tcpTryCount);
    final YarnJobSubmissionClient client = Tang.Factory.getTang()
        .newInjector(yarnConfiguration)
        .getInstance(YarnJobSubmissionClient.class);

    client.launch(driverFolder, jobId, priority, queue, driverMemory);
  }
}
