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
package org.apache.reef.services.network;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.NetworkServiceConfigurationBuilder;
import org.apache.reef.io.network.config.NetworkServiceConfigurationBuilderImpl;
import org.apache.reef.io.network.config.parameters.NetworkServiceParameters;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.services.network.util.IntegerCodec;
import org.apache.reef.services.network.util.Monitor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network service test
 */
public class NetworkServiceTest {
  private static final Logger LOG = Logger.getLogger(NetworkServiceTest.class.getName());

  private final LocalAddressProvider localAddressProvider;
  private final String localAddress;

  public NetworkServiceTest() throws InjectionException {
    localAddressProvider = LocalAddressProviderFactory.getInstance();
    localAddress = localAddressProvider.getLocalAddress();
  }

  @Rule
  public TestName name = new TestName();

  public static final class GroupCommClientId implements Name<String> {
  }

  public static final class ShuffleClientId implements Name<String> {

  }

  /**
   * NetworkService messaging test
   */
  @Test
  public void testMessagingNetworkService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer nameServer = injector.getInstance(NameServer.class);
    NetworkServiceConfigurationBuilder serviceDriver = injector.getInstance(NetworkServiceConfigurationBuilderImpl.class);

    // task2 conf
    serviceDriver.setConnectionFactory("task2", GroupCommClientId.class, StringCodec.class,
        StringMessagingHandler.class);

    serviceDriver.setConnectionFactory("task1", GroupCommClientId.class, StringCodec.class,
        DefaultHandler.class);


    final int numMessages = 2000;
    final Monitor monitor = new Monitor();

    Configuration conf = serviceDriver.getServiceConfiguration("task2", GroupCommClientId.class);
    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service for task2
    Injector ij2 = injector.forkInjector(conf);
    ij2.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
    ij2.bindVolatileParameter(StringMessagingHandler.StringExpected.class, numMessages);
    final NetworkService ns2 = ij2.getInstance(NetworkService.class);
    IdentifierFactory factory = ij2.getNamedInstance(NetworkServiceParameters.IdentifierFactory.class);
    ns2.registerId(factory.getNewInstance("task2"));

    // connection for receiving messages
    ConnectionFactory<String> receiver = ns2.newConnectionFactory(GroupCommClientId.class);

    // network service for task1
    LOG.log(Level.FINEST, "=== Test network service sender start");
    Configuration conf2 = serviceDriver.getServiceConfiguration("task1", GroupCommClientId.class);
    Injector ij3 = injector.forkInjector(conf2);
    final NetworkService ns1 = ij3.getInstance(NetworkService.class);
    ns1.registerId(factory.getNewInstance("task1"));
    // connection for sending messages
    ConnectionFactory<String> sender = ns1.newConnectionFactory(GroupCommClientId.class);

    final Identifier destId = factory.getNewInstance("task2");
    final org.apache.reef.io.network.Connection<String> conn = sender.newConnection(destId);
    try {
      conn.open();
      for (int count = 0; count < numMessages; ++count) {
        conn.write("hello! " + count);
      }
      monitor.mwait();

    } catch (NetworkException e) {
      e.printStackTrace();
    }
    conn.close();

    ns1.close();
    ns2.close();

    nameServer.close();
  }

  /**
   * Multiple connection factories test
   */
  @Test
  public void testMultipleConnectionFactoriesTest() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    ExecutorService executor = Executors.newFixedThreadPool(5);

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer nameServer = injector.getInstance(NameServer.class);
    NetworkServiceConfigurationBuilder serviceDriver = injector.getInstance(NetworkServiceConfigurationBuilderImpl.class);

    // task2 conf
    serviceDriver.setConnectionFactory("task2", GroupCommClientId.class, StringCodec.class,
        StringMessagingHandler.class);
    serviceDriver.setConnectionFactory("task2", ShuffleClientId.class, IntegerCodec.class,
        IntegerMessagingHandler.class);

    serviceDriver.setConnectionFactory("task1", GroupCommClientId.class, StringCodec.class,
        DefaultHandler.class);
    serviceDriver.setConnectionFactory("task1", ShuffleClientId.class, IntegerCodec.class,
        DefaultHandler.class);

    // task2 conf
    Configuration conf = serviceDriver.getServiceConfiguration("task2", GroupCommClientId.class);
    Configuration conf11 = serviceDriver.getServiceConfiguration("task2", ShuffleClientId.class);

    // task1 conf
    Configuration conf2 = serviceDriver.getServiceConfiguration("task1", GroupCommClientId.class);
    Configuration conf22 = serviceDriver.getServiceConfiguration("task1", ShuffleClientId.class);

    final int groupcommMessages = 1000;
    final int shuffleMessges = 2000;
    final Monitor monitor = new Monitor();
    final Monitor monitor2 = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service for task2
    Injector ij2 = injector.forkInjector(conf, conf11);
    ij2.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
    ij2.bindVolatileParameter(StringMessagingHandler.StringExpected.class, groupcommMessages);
    ij2.bindVolatileParameter(IntegerMessagingHandler.IntegerMonitor.class, monitor2);
    ij2.bindVolatileParameter(IntegerMessagingHandler.IntegerExpected.class, shuffleMessges);
    final NetworkService ns2 = ij2.getInstance(NetworkService.class);
    IdentifierFactory factory = ij2.getNamedInstance(NetworkServiceParameters.IdentifierFactory.class);
    ns2.registerId(factory.getNewInstance("task2"));

    // connection for receiving messages
    ConnectionFactory<String> gcReceiver = ns2.newConnectionFactory(GroupCommClientId.class);
    ConnectionFactory<Integer> shuffleReceiver = ns2.newConnectionFactory(ShuffleClientId.class);

    // network service for task1
    LOG.log(Level.FINEST, "=== Test network service sender start");
    Injector ij3 = injector.forkInjector(conf2, conf22);
    final NetworkService ns1 = ij3.getInstance(NetworkService.class);

    ns1.registerId(factory.getNewInstance("task1"));

    // connection for sending messages
    ConnectionFactory<String> gcSender = ns1.newConnectionFactory(GroupCommClientId.class);
    ConnectionFactory<Integer> shuffleSender = ns1.newConnectionFactory(ShuffleClientId.class);

    final Identifier destId = factory.getNewInstance("task2");
    final Connection<String> conn = gcSender.newConnection(destId);
    final Connection<Integer> conn2 = shuffleSender.newConnection(destId);
    try {
      conn.open();
      conn2.open();

      executor.submit(new Runnable() {
        @Override
        public void run() {
          for (int count = 0; count < groupcommMessages; ++count) {
            try {
              conn.write("hello! " + count);
            } catch (NetworkException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

      executor.submit(new Runnable() {
        @Override
        public void run() {
          for (int count = 0; count < shuffleMessges; ++count) {
            try {
              conn2.write(count);
            } catch (NetworkException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

      monitor.mwait();
      monitor2.mwait();

    } catch (NetworkException e) {
      e.printStackTrace();
    }
    conn.close();

    ns1.close();
    ns2.close();

    executor.shutdown();
    nameServer.close();
  }

  /**
   * NetworkService messaging rate benchmark
   */
  @Test
  public void testMessagingNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer server = injector.getInstance(NameServer.class);

    final int[] messageSizes = {1, 16, 32, 64, 512, 64 * 1024, 1024 * 1024};

    for (int size : messageSizes) {
      final IdentifierFactory factory = new StringIdentifierFactory();

      final int numMessages = 300000 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();
      injector = Tang.Factory.getTang().newInjector();
      final NetworkServiceConfigurationBuilder serviceDriver = injector.getInstance(NetworkServiceConfigurationBuilderImpl.class);

      // network service
      final String name2 = "receiver";
      final String name1 = "sender";
      serviceDriver.setConnectionFactory(name2, GroupCommClientId.class, StringCodec.class, StringMessagingHandler.class);
      serviceDriver.setConnectionFactory(name1, GroupCommClientId.class, StringCodec.class, DefaultHandler.class);

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      Injector ij2 = injector.forkInjector(serviceDriver.getServiceConfiguration(name2, GroupCommClientId.class));
      ij2.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
      ij2.bindVolatileParameter(StringMessagingHandler.StringExpected.class, numMessages);
      NetworkService ns2 = ij2.getInstance(NetworkService.class);
      ns2.registerId(factory.getNewInstance(name2));


      LOG.log(Level.FINEST, "=== Test network service sender start");
      Injector ij1 = injector.forkInjector(serviceDriver.getServiceConfiguration(name1, GroupCommClientId.class));
      NetworkService ns1 = ij1.getInstance(NetworkService.class);
      ns1.registerId(factory.getNewInstance(name1));

      Identifier destId = factory.getNewInstance(name2);
      ConnectionFactory<String> connFactory = ns1.newConnectionFactory(GroupCommClientId.class);
      Connection<String> conn = connFactory.newConnection(destId);

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      String message = msb.toString();

      long start = System.currentTimeMillis();
      try {
        for (int i = 0; i < numMessages; i++) {
          conn.open();
          conn.write(message);
        }
        monitor.mwait();
      } catch (NetworkException e) {
        e.printStackTrace();
      }
      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;
      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + numMessages / runtime + " bandwidth(bytes/s): " + ((double) numMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();

      ns1.close();
      ns2.close();

    }
    server.close();

  }

  /**
   * NetworkService messaging rate benchmark
   */
  @Test
  public void testMessagingNetworkServiceRateDisjoint() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    final IdentifierFactory factory = new StringIdentifierFactory();

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer server = injector.getInstance(NameServer.class);

    BlockingQueue<Object> barrier = new LinkedBlockingQueue<Object>();

    int numThreads = 4;
    final int size = 2000;
    final int numMessages = 300000 / (Math.max(1, size / 512));
    final int totalNumMessages = numMessages * numThreads;


    ExecutorService e = Executors.newCachedThreadPool();
    for (int t = 0; t < numThreads; t++) {
      final int tt = t;

      e.submit(new Runnable() {
        public void run() {
          try {
            Monitor monitor = new Monitor();
            Injector injector = Tang.Factory.getTang().newInjector();
            final NetworkServiceConfigurationBuilder serviceDriver = injector.getInstance(NetworkServiceConfigurationBuilderImpl.class);

            // network service
            final String name2 = "task2-" + tt;
            final String name1 = "task1-" + tt;

            serviceDriver.setConnectionFactory(name2, GroupCommClientId.class, StringCodec.class,
                StringMessagingHandler.class);
            serviceDriver.setConnectionFactory(name1, GroupCommClientId.class, StringCodec.class,
                DefaultHandler.class);

            LOG.log(Level.FINEST, "=== Test network service receiver start");
            Injector ij2 = injector.forkInjector(serviceDriver.getServiceConfiguration(name2, GroupCommClientId.class));
            ij2.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
            ij2.bindVolatileParameter(StringMessagingHandler.StringExpected.class, numMessages);
            final NetworkService ns2 = ij2.getInstance(NetworkService.class);
            ns2.registerId(factory.getNewInstance(name2));
            ConnectionFactory<String> receiver = ns2.newConnectionFactory(GroupCommClientId.class);

            LOG.log(Level.FINEST, "=== Test network service sender start");
            Injector ij3 = injector.forkInjector(serviceDriver.getServiceConfiguration(name1, GroupCommClientId.class));
            final NetworkService ns1 = ij3.getInstance(NetworkService.class);
            ns1.registerId(factory.getNewInstance(name1));
            ConnectionFactory<String> sender = ns1.newConnectionFactory(GroupCommClientId.class);
            Identifier destId = factory.getNewInstance(name2);
            Connection<String> conn = sender.newConnection(destId);

            // build the message
            StringBuilder msb = new StringBuilder();
            for (int i = 0; i < size; i++) {
              msb.append("1");
            }
            String message = msb.toString();


            try {
              for (int i = 0; i < numMessages; i++) {
                conn.open();
                conn.write(message);
              }
              monitor.mwait();
            } catch (NetworkException e) {
              e.printStackTrace();
            }
            conn.close();

            ns1.close();
            ns2.close();
          } catch (Exception e) {
            e.printStackTrace();

          }
        }
      });
    }

    // start and time
    long start = System.currentTimeMillis();
    Object ignore = new Object();
    for (int i = 0; i < numThreads; i++) barrier.add(ignore);
    e.shutdown();
    e.awaitTermination(100, TimeUnit.SECONDS);
    long end = System.currentTimeMillis();

    double runtime = ((double) end - start) / 1000;
    LOG.log(Level.INFO, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars

    server.close();
  }

  @Test
  public void testMultithreadedSharedConnMessagingNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    final IdentifierFactory factory = new StringIdentifierFactory();

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer server = injector.getInstance(NameServer.class);


    final int[] messageSizes = {2000};// {1,16,32,64,512,64*1024,1024*1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      int numThreads = 2;
      int totalNumMessages = numMessages * numThreads;
      final Monitor monitor = new Monitor();
      injector = Tang.Factory.getTang().newInjector();
      final NetworkServiceConfigurationBuilder serviceDriver = injector.getInstance(NetworkServiceConfigurationBuilderImpl.class);

      final String name2 = "receiver";
      final String name1 = "sender";

      serviceDriver.setConnectionFactory(name2, GroupCommClientId.class, StringCodec.class,
          StringMessagingHandler.class);
      serviceDriver.setConnectionFactory(name1, GroupCommClientId.class, StringCodec.class,
          DefaultHandler.class);

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      Injector ij2 = injector.forkInjector(serviceDriver.getServiceConfiguration(name2, GroupCommClientId.class));
      ij2.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
      ij2.bindVolatileParameter(StringMessagingHandler.StringExpected.class, totalNumMessages);
      NetworkService ns2 = ij2.getInstance(NetworkService.class);
      ns2.registerId(factory.getNewInstance(name2));


      LOG.log(Level.FINEST, "=== Test network service sender start");
      Injector ij1 = injector.forkInjector(serviceDriver.getServiceConfiguration(name1, GroupCommClientId.class));
      NetworkService ns1 = ij1.getInstance(NetworkService.class);
      ns1.registerId(factory.getNewInstance(name1));

      Identifier destId = factory.getNewInstance(name2);
      ConnectionFactory<String> connFactory = ns1.newConnectionFactory(GroupCommClientId.class);
      final Connection<String> conn = connFactory.newConnection(destId);
      conn.open();

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      final String message = msb.toString();

      ExecutorService e = Executors.newCachedThreadPool();

      long start = System.currentTimeMillis();
      for (int i = 0; i < numThreads; i++) {
        e.submit(new Runnable() {

          @Override
          public void run() {
            try {
              for (int i = 0; i < numMessages; i++) {
                conn.write(message);
              }
            } catch (NetworkException e) {
              e.printStackTrace();
            }
          }
        });
      }


      e.shutdown();
      e.awaitTermination(30, TimeUnit.SECONDS);
      monitor.mwait();

      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;

      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();

      ns1.close();
      ns2.close();
    }

    server.close();
  }

  /**
   * NetworkService messaging rate benchmark
   */
  @Test
  public void testMessagingNetworkServiceBatchingRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer server = injector.getInstance(NameServer.class);

    final int batchSize = 1024 * 1024;
    final int[] messageSizes = {32, 64, 512};

    for (int size : messageSizes) {
      final int numMessages = 300 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      injector = Tang.Factory.getTang().newInjector();
      final NetworkServiceConfigurationBuilder serviceDriver = injector.getInstance(NetworkServiceConfigurationBuilderImpl.class);

      final String name2 = "receiver";
      final String name1 = "sender";

      serviceDriver.setConnectionFactory(name2, GroupCommClientId.class, StringCodec.class,
          StringMessagingHandler.class);
      serviceDriver.setConnectionFactory(name1, GroupCommClientId.class, StringCodec.class,
          DefaultHandler.class);

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      Injector ij2 = injector.forkInjector(serviceDriver.getServiceConfiguration(name2, GroupCommClientId.class));
      ij2.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
      ij2.bindVolatileParameter(StringMessagingHandler.StringExpected.class, numMessages);
      NetworkService ns2 = ij2.getInstance(NetworkService.class);
      ns2.registerId(factory.getNewInstance(name2));


      LOG.log(Level.FINEST, "=== Test network service sender start");
      Injector ij1 = injector.forkInjector(serviceDriver.getServiceConfiguration(name1, GroupCommClientId.class));
      NetworkService ns1 = ij1.getInstance(NetworkService.class);
      ns1.registerId(factory.getNewInstance(name1));

      Identifier destId = factory.getNewInstance(name2);
      ConnectionFactory<String> connFactory = ns1.newConnectionFactory(GroupCommClientId.class);
      final Connection<String> conn = connFactory.newConnection(destId);
      conn.open();
      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      String message = msb.toString();

      long start = System.currentTimeMillis();
      try {
        for (int i = 0; i < numMessages; i++) {
          StringBuilder sb = new StringBuilder();
          for (int j = 0; j < batchSize / size; j++) {
            sb.append(message);
          }
          conn.open();
          conn.write(sb.toString());
        }
        monitor.mwait();
      } catch (NetworkException e) {
        e.printStackTrace();
      }
      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;
      long numAppMessages = numMessages * batchSize / size;
      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + numAppMessages / runtime + " bandwidth(bytes/s): " + ((double) numAppMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();

      ns1.close();
      ns2.close();
    }

    server.close();
  }
}
