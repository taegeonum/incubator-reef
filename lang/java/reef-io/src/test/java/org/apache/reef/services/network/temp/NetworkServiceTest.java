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
import org.apache.reef.io.network.temp.NetworkServiceParameters;
import org.apache.reef.io.network.temp.impl.NetworkEvent;
import org.apache.reef.io.network.temp.impl.NetworkServiceConfiguration;
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.services.network.util.Monitor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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

  /**
   * NetworkService messaging test
   */
  @Test
  public void testMessagingNetworkService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    // name server
    Injector injector = Tang.Factory.getTang().newInjector();
    NameServer nameServer = injector.getInstance(NameServer.class);
    int port = nameServer.getPort();

    // task2 conf
    Configuration conf = NetworkServiceConfiguration.CONF
        .set(NetworkServiceConfiguration.NAME_SERVICE_ADDRESS, localAddress)
        .set(NetworkServiceConfiguration.NAME_SERVER_IDENTIFIER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NETWORK_SERVICE_IDENDITIFER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NAME_SERVICE_PORT, port)
        .build();

    // task1 conf
    Configuration conf2 = NetworkServiceConfiguration.CONF
        .set(NetworkServiceConfiguration.NAME_SERVICE_ADDRESS, localAddress)
        .set(NetworkServiceConfiguration.NAME_SERVER_IDENTIFIER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NETWORK_SERVICE_IDENDITIFER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NAME_SERVICE_PORT, port)
        .build();

    final int numMessages = 2000;
    final Monitor monitor = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service for task2
    Injector ij2 = injector.forkInjector(conf);
    final NetworkService ns2 = ij2.getInstance(NetworkService.class);
    IdentifierFactory factory = ij2.getNamedInstance(NetworkServiceParameters.IdentifierFactory.class);
    ns2.registerId(factory.getNewInstance("task2"));

    // connection for receiving messages
    ConnectionFactory<String> receiver = ns2.newConnectionFactory(factory.getNewInstance("GroupComm"), new StringCodec(), new MessageHandler<String>("task2", monitor, numMessages));

    // network service for task1
    LOG.log(Level.FINEST, "=== Test network service sender start");
    Injector ij3 = injector.forkInjector(conf2);
    final NetworkService ns1 = ij3.getInstance(NetworkService.class);
    ns1.registerId(factory.getNewInstance("task1"));
    // connection for sending messages
    ConnectionFactory<String> sender = ns1.newConnectionFactory(factory.getNewInstance("GroupComm"), new StringCodec(), new MessageHandler<String>("task1", monitor, numMessages));

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
    int port = nameServer.getPort();

    // task2 conf
    Configuration conf = NetworkServiceConfiguration.CONF
        .set(NetworkServiceConfiguration.NAME_SERVICE_ADDRESS, localAddress)
        .set(NetworkServiceConfiguration.NAME_SERVER_IDENTIFIER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NETWORK_SERVICE_IDENDITIFER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NAME_SERVICE_PORT, port)
        .build();

    // task1 conf
    Configuration conf2 = NetworkServiceConfiguration.CONF
        .set(NetworkServiceConfiguration.NAME_SERVICE_ADDRESS, localAddress)
        .set(NetworkServiceConfiguration.NAME_SERVER_IDENTIFIER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NETWORK_SERVICE_IDENDITIFER_FACTORY, StringIdentifierFactory.class)
        .set(NetworkServiceConfiguration.NAME_SERVICE_PORT, port)
        .build();

    final int groupcommMessages = 1000;
    final int shuffleMessges = 2000;
    final Monitor monitor = new Monitor();
    final Monitor monitor2 = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service for task2
    Injector ij2 = injector.forkInjector(conf);
    final NetworkService ns2 = ij2.getInstance(NetworkService.class);
    IdentifierFactory factory = ij2.getNamedInstance(NetworkServiceParameters.IdentifierFactory.class);
    ns2.registerId(factory.getNewInstance("task2"));

    // connection for receiving messages
    ConnectionFactory<String> gcReceiver = ns2.newConnectionFactory(factory.getNewInstance("GroupComm"), new StringCodec(), new MessageHandler<String>("task2", monitor, groupcommMessages));
    ConnectionFactory<Integer> shuffleReceiver = ns2.newConnectionFactory(factory.getNewInstance("Shuffle"), new ObjectSerializableCodec<Integer>(), new MessageHandler<Integer>("task2", monitor2, shuffleMessges));

    // network service for task1
    LOG.log(Level.FINEST, "=== Test network service sender start");
    Injector ij3 = injector.forkInjector(conf2);
    final NetworkService ns1 = ij3.getInstance(NetworkService.class);
    ns1.registerId(factory.getNewInstance("task1"));

    // connection for sending messages
    ConnectionFactory<String> gcSender = ns1.newConnectionFactory(factory.getNewInstance("GroupComm"), new StringCodec(), new MessageHandler<String>("task1", monitor, groupcommMessages));
    ConnectionFactory<Integer> shuffleSender = ns1.newConnectionFactory(factory.getNewInstance("Shuffle"), new ObjectSerializableCodec<Integer>(), new MessageHandler<Integer>("task1", monitor2, shuffleMessges));

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
   * Test message handler
   */
  class MessageHandler<T> implements EventHandler<NetworkEvent<T>> {

    private final String name;
    private final int expected;
    private final Monitor monitor;
    private AtomicInteger count = new AtomicInteger(0);

    MessageHandler(String name, Monitor monitor, int expected) {
      this.name = name;
      this.monitor = monitor;
      this.expected = expected;
    }

    @Override
    public void onNext(NetworkEvent<T> value) {
      count.incrementAndGet();
      LOG.log(Level.INFO,
          "OUT: {0} received {1} from {2} to {3}",
          new Object[]{name, value, value.getSrcId(), value.getDestId()});

      for (final T obj : value.getData()) {
        LOG.log(Level.INFO, "OUT: data: {0}", obj);
      }

      if (count.get() == expected) {
        monitor.mnotify();
      }
    }
  }

  /**
   * Test exception handler
   */
  class ExceptionHandler implements EventHandler<Exception> {
    @Override
    public void onNext(Exception error) {
      System.err.println(error);
    }
  }

}
