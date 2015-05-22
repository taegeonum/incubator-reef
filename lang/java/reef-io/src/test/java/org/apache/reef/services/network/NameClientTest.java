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
package org.apache.reef.services.network;

import org.apache.reef.io.network.naming.*;
import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.NettyNioEventLoopGroupProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

public class NameClientTest {

  private static final LocalAddressProvider localAddressProvider;
  private static final TransportFactory tpFactory;
  private static final NettyNioEventLoopGroupProvider sharedNioEventLoopGroup;

  static int retryCount, retryTimeout;

  public NameClientTest() throws InjectionException {
  }

  static {
    Tang tang = Tang.Factory.getTang();
    try {
      retryCount = tang.newInjector().getNamedInstance(NameLookupClient.RetryCount.class);
      retryTimeout = tang.newInjector().getNamedInstance(NameLookupClient.RetryTimeout.class);
      final Injector injector = Tang.Factory.getTang().newInjector();
      localAddressProvider = injector.getInstance(LocalAddressProvider.class);
      tpFactory = injector.getInstance(TransportFactory.class);
      sharedNioEventLoopGroup = injector.getInstance(NettyNioEventLoopGroupProvider.class);

    } catch (InjectionException e1) {
      throw new RuntimeException("Exception while trying to find default values for retryCount & Timeout", e1);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }


  @AfterClass
  public static void cleanupInstances() throws Exception {
    sharedNioEventLoopGroup.close();
  }

  /**
   * Test method for {@link org.apache.reef.io.network.naming.NameClient#close()}.
   *
   * @throws Exception
   */
  @Test
  public final void testClose() throws Exception {
    final String localAddress = localAddressProvider.getLocalAddress();
    IdentifierFactory factory = new StringIdentifierFactory();
    try (NameServer server = new NameServerImpl(0, factory, this.localAddressProvider, tpFactory)) {
      int serverPort = server.getPort();
      try (NameClient client = new NameClient(localAddress, serverPort, 1000, factory, retryCount, retryTimeout,
          new NameCache(10000), localAddressProvider)) {
        Identifier id = factory.getNewInstance("Task1");
        client.register(id, new InetSocketAddress(localAddress, 7001));
        client.unregister(id);
        Thread.sleep(100);
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.naming.NameClient#lookup()}.
   * To check caching behavior with expireAfterAccess & expireAfterWrite
   * Changing NameCache's pattern to expireAfterAccess causes this test to fail
   *
   * @throws Exception
   */
  @Test
  public final void testLookup() throws Exception {
    IdentifierFactory factory = new StringIdentifierFactory();
    final String localAddress = localAddressProvider.getLocalAddress();
    try (NameServer server = new NameServerImpl(0, factory, this.localAddressProvider, tpFactory)) {
      int serverPort = server.getPort();
      try (NameClient client = new NameClient(localAddress, serverPort, 1000,  factory, retryCount, retryTimeout,
          new NameCache(150), localAddressProvider, tpFactory)) {
        Identifier id = factory.getNewInstance("Task1");
        client.register(id, new InetSocketAddress(localAddress, 7001));
        client.lookup(id);// caches the entry
        client.unregister(id);
        Thread.sleep(100);
        try {
          InetSocketAddress addr = client.lookup(id);
          Thread.sleep(100);
          //With expireAfterAccess, the previous lookup would reset expiry to 150ms
          //more and 100ms wait will not expire the item and will return the cached value
          //With expireAfterWrite, the extra wait of 100 ms will expire the item
          //resulting in NamingException and the test passes
          addr = client.lookup(id);
          Assert.assertNull("client.lookup(id)", addr);
        } catch (Exception e) {
          if (e instanceof ExecutionException) {
            Assert.assertTrue("Execution Exception cause is instanceof NamingException", e.getCause() instanceof NamingException);
          } else
            throw e;
        }
      }
    }
  }

}
