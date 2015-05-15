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
package org.apache.reef.services.network.temp;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.temp.NetworkService;
import org.apache.reef.io.network.temp.NetworkServiceDriver;
import org.apache.reef.io.network.temp.NetworkServiceParameters;
import org.apache.reef.services.network.util.IntegerCodec;
import org.apache.reef.io.network.util.StringCodec;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    NetworkServiceDriver serviceDriver = injector.getInstance(NetworkServiceDriver.class);

    // task2 conf
    serviceDriver.setConnectionFactory("task2", GroupCommClientId.class, StringCodec.class,
        StringMessagingHandler.class);

    serviceDriver.setConnectionFactory("task1", GroupCommClientId.class, StringCodec.class,
        StringMessagingHandler.class);


    final int numMessages = 2000;
    final Monitor monitor = new Monitor();

    Configuration conf = serviceDriver.getServiceConfiguration("task2");
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
    Configuration conf2 = serviceDriver.getServiceConfiguration("task1");
    Injector ij3 = injector.forkInjector(conf2);
    ij3.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
    ij3.bindVolatileParameter(StringMessagingHandler.StringExpected.class, numMessages);
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
   * Multiple client services test
   */
  @Test
  public void testMultipleClientNetworkService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    ExecutorService executor = Executors.newFixedThreadPool(5);

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer nameServer = injector.getInstance(NameServer.class);
    NetworkServiceDriver serviceDriver = injector.getInstance(NetworkServiceDriver.class);

    // task2 conf
    serviceDriver.setConnectionFactory("task2", GroupCommClientId.class, StringCodec.class,
        StringMessagingHandler.class);
    serviceDriver.setConnectionFactory("task2", ShuffleClientId.class, IntegerCodec.class,
        IntegerMessagingHandler.class);

    serviceDriver.setConnectionFactory("task1", GroupCommClientId.class, StringCodec.class,
        StringMessagingHandler.class);
    serviceDriver.setConnectionFactory("task1", ShuffleClientId.class, IntegerCodec.class,
        IntegerMessagingHandler.class);

    // task2 conf
    Configuration conf = serviceDriver.getServiceConfiguration("task2");

    // task1 conf
    Configuration conf2 = serviceDriver.getServiceConfiguration("task1");

    final int groupcommMessages = 1000;
    final int shuffleMessges = 2000;
    final Monitor monitor = new Monitor();
    final Monitor monitor2 = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service for task2
    Injector ij2 = injector.forkInjector(conf);
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
    Injector ij3 = injector.forkInjector(conf2);
    ij3.bindVolatileParameter(StringMessagingHandler.StringMonitor.class, monitor);
    ij3.bindVolatileParameter(StringMessagingHandler.StringExpected.class, groupcommMessages);
    ij3.bindVolatileParameter(IntegerMessagingHandler.IntegerMonitor.class, monitor2);
    ij3.bindVolatileParameter(IntegerMessagingHandler.IntegerExpected.class, shuffleMessges);
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
}
