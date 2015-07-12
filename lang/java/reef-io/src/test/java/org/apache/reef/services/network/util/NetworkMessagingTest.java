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
package org.apache.reef.services.network.util;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class for NetworkConnectionService test.
 */
public final class NetworkMessagingTest {
  private static final Logger LOG = Logger.getLogger(NetworkMessagingTest.class.getName());

  private final IdentifierFactory factory;
  private final NetworkConnectionService receiverNetworkConnService;
  private final NetworkConnectionService senderNetworkConnService;
  private final String receiver;
  private final String sender;
  private final NameServer nameServer;

  public NetworkMessagingTest(final String localAddress) throws InjectionException {
    // name server
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.nameServer = injector.getInstance(NameServer.class);
    final Configuration netConf = NameResolverConfiguration.CONF
        .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
        .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
        .build();

    LOG.log(Level.FINEST, "=== Test network connection service receiver start");
    // network service for receiver
    this.receiver = "receiver";
    final Injector injectorReceiver = injector.forkInjector(netConf);
    this.receiverNetworkConnService = injectorReceiver.getInstance(NetworkConnectionService.class);
    this.factory = injectorReceiver.getNamedInstance(NetworkConnectionServiceIdFactory.class);
    this.receiverNetworkConnService.registerId(this.factory.getNewInstance(receiver));

    // network service for sender
    this.sender = "sender";
    LOG.log(Level.FINEST, "=== Test network connection service sender start");
    final Injector injectorSender = injector.forkInjector(netConf);
    senderNetworkConnService = injectorSender.getInstance(NetworkConnectionService.class);
    senderNetworkConnService.registerId(this.factory.getNewInstance(this.sender));
  }

  public <T> void registerTestConnectionFactory(final Identifier connFactoryId,
                                                final int numMessages, final Monitor monitor,
                                                final Codec<T> codec) throws NetworkException {
    receiverNetworkConnService.registerConnectionFactory(connFactoryId, codec, new MessageHandler<T>(monitor, numMessages), new TestListener<T>());
    senderNetworkConnService.registerConnectionFactory(connFactoryId, codec, new MessageHandler<T>(monitor, numMessages), new TestListener<T>());
  }

  public <T> Connection<T> getConnectionFromSenderToReceiver(final Identifier connFactoryId) {
    final Identifier destId = factory.getNewInstance(receiver);
    return (Connection<T>)senderNetworkConnService.getConnectionFactory(connFactoryId).newConnection(destId);
  }

  public void close() throws Exception {
    senderNetworkConnService.close();
    receiverNetworkConnService.close();
    nameServer.close();
  }

  public static final class MessageHandler<T> implements EventHandler<Message<T>> {
    private final int expected;
    private final Monitor monitor;
    private AtomicInteger count = new AtomicInteger(0);

    public MessageHandler(final Monitor monitor,
                          final int expected) {
      this.monitor = monitor;
      this.expected = expected;
    }

    @Override
    public void onNext(Message<T> value) {
      count.incrementAndGet();
      LOG.log(Level.FINE, "Count: {0}", count.get());
      LOG.log(Level.FINE,
          "OUT: {0} received {1} from {2} to {3}",
          new Object[]{value, value.getSrcId(), value.getDestId()});

      for (final T obj : value.getData()) {
        LOG.log(Level.FINE, "OUT: data: {0}", obj);
      }

      if (count.get() == expected) {
        monitor.mnotify();
      }
    }
  }

  public static final class TestListener<T> implements LinkListener<Message<T>> {
    @Override
    public void onSuccess(Message<T> message) {
      LOG.log(Level.FINE, "success: " + message);
    }
    @Override
    public void onException(Throwable cause, SocketAddress remoteAddress, Message<T> message) {
      LOG.log(Level.WARNING, "exception: " + cause + message);
      throw new RuntimeException(cause);
    }
  }
}