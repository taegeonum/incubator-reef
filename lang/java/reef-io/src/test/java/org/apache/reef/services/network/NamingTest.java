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

import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.io.network.naming.*;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryCount;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryTimeout;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming server and client test
 */
public class NamingTest {

  private static final Logger LOG = Logger.getLogger(NamingTest.class.getName());
  private static final int retryCount;
  private static final int retryTimeout;

  static {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      retryCount = injector.getNamedInstance(NameResolverRetryCount.class);
      retryTimeout = injector.getNamedInstance(NameResolverRetryTimeout.class);
    } catch (final InjectionException ex) {
      final String msg = "Exception while trying to find default values for retryCount & Timeout";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  private final LocalAddressProvider localAddressProvider;
  @Rule
  public final TestName name = new TestName();
  final long TTL = 30000;
  final IdentifierFactory factory = new StringIdentifierFactory();
  int port;

  public NamingTest() throws InjectionException {
    this.localAddressProvider = LocalAddressProviderFactory.getInstance();
  }

  /**
   * NameServer and NameLookupClient test
   *
   * @throws Exception
   */
  @Test
  public void testNamingLookup() throws Exception {

    final String localAddress = localAddressProvider.getLocalAddress();
    LOG.log(Level.FINEST, this.name.getMethodName());

    // names 
    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));

    // run a server
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    this.port = server.getPort();
    for (final Identifier id : idToAddrMap.keySet()) {
      server.register(id, idToAddrMap.get(id));
    }

    // run a client
    final NameLookupClient client = new NameLookupClient(localAddress, this.port,
        10000, this.factory, retryCount, retryTimeout, new NameCache(this.TTL), this.localAddressProvider);

    final Identifier id1 = this.factory.getNewInstance("task1");
    final Identifier id2 = this.factory.getNewInstance("task2");

    final Map<Identifier, InetSocketAddress> respMap = new HashMap<Identifier, InetSocketAddress>();
    InetSocketAddress addr1 = client.lookup(id1);
    respMap.put(id1, addr1);
    InetSocketAddress addr2 = client.lookup(id2);
    respMap.put(id2, addr2);

    for (final Identifier id : respMap.keySet()) {
      LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[]{id, respMap.get(id)});
    }

    Assert.assertTrue(isEqual(idToAddrMap, respMap));

    client.close();
    server.close();
  }

  /**
   * Test concurrent lookups (threads share a client)
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentNamingLookup() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final String localAddress = localAddressProvider.getLocalAddress();
    // test it 3 times to make failure likely
    for (int i = 0; i < 3; i++) {

      LOG.log(Level.FINEST, "test {0}", i);

      // names 
      final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
      idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
      idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));
      idToAddrMap.put(this.factory.getNewInstance("task3"), new InetSocketAddress(localAddress, 7003));

      // run a server
      final Injector injector = Tang.Factory.getTang().newInjector();
      injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
      injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
      final NameServer server = injector.getInstance(NameServer.class);
      this.port = server.getPort();
      for (final Identifier id : idToAddrMap.keySet()) {
        server.register(id, idToAddrMap.get(id));
      }

      // run a client
      final NameLookupClient client = new NameLookupClient(localAddress, this.port,
          10000, this.factory, retryCount, retryTimeout, new NameCache(this.TTL), this.localAddressProvider);

      final Identifier id1 = this.factory.getNewInstance("task1");
      final Identifier id2 = this.factory.getNewInstance("task2");
      final Identifier id3 = this.factory.getNewInstance("task3");

      final ExecutorService e = Executors.newCachedThreadPool();

      final ConcurrentMap<Identifier, InetSocketAddress> respMap = new ConcurrentHashMap<Identifier, InetSocketAddress>();

      final Future<?> f1 = e.submit(new Runnable() {
        @Override
        public void run() {
          InetSocketAddress addr = null;
          try {
            addr = client.lookup(id1);
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Lookup failed", e);
            Assert.fail(e.toString());
          }
          respMap.put(id1, addr);
        }
      });
      final Future<?> f2 = e.submit(new Runnable() {
        @Override
        public void run() {
          InetSocketAddress addr = null;
          try {
            addr = client.lookup(id2);
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Lookup failed", e);
            Assert.fail(e.toString());
          }
          respMap.put(id2, addr);
        }
      });
      final Future<?> f3 = e.submit(new Runnable() {
        @Override
        public void run() {
          InetSocketAddress addr = null;
          try {
            addr = client.lookup(id3);
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Lookup failed", e);
            Assert.fail(e.toString());
          }
          respMap.put(id3, addr);
        }
      });

      f1.get();
      f2.get();
      f3.get();

      for (final Identifier id : respMap.keySet()) {
        LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[]{id, respMap.get(id)});
      }

      Assert.assertTrue(isEqual(idToAddrMap, respMap));

      client.close();
      server.close();
    }
  }

  /**
   * NameServer and NameRegistryClient test
   *
   * @throws Exception
   */
  @Test
  public void testNamingRegistry() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    this.port = server.getPort();
    final String localAddress = localAddressProvider.getLocalAddress();

    // names to start with
    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));

    // registration
    // invoke registration from the client side
    final NameRegistryClient client = new NameRegistryClient(localAddress, this.port, this.factory, this.localAddressProvider);
    for (final Identifier id : idToAddrMap.keySet()) {
      client.register(id, idToAddrMap.get(id));
    }

    // wait
    final Set<Identifier> ids = idToAddrMap.keySet();
    busyWait(server, ids.size(), ids);

    // check the server side 
    Map<Identifier, InetSocketAddress> serverMap = new HashMap<Identifier, InetSocketAddress>();
    Iterable<NameAssignment> nas = server.lookup(ids);

    for (final NameAssignment na : nas) {
      LOG.log(Level.FINEST, "Mapping: {0} -> {1}",
          new Object[]{na.getIdentifier(), na.getAddress()});
      serverMap.put(na.getIdentifier(), na.getAddress());
    }

    Assert.assertTrue(isEqual(idToAddrMap, serverMap));

    // un-registration
    for (final Identifier id : idToAddrMap.keySet()) {
      client.unregister(id);
    }

    // wait
    busyWait(server, 0, ids);

    serverMap = new HashMap<Identifier, InetSocketAddress>();
    nas = server.lookup(ids);
    for (final NameAssignment na : nas)
      serverMap.put(na.getIdentifier(), na.getAddress());

    Assert.assertEquals(0, serverMap.size());

    client.close();
    server.close();
  }

  /**
   * NameServer and NameClient test
   *
   * @throws Exception
   */
  @Test
  public void testNameClient() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final String localAddress = localAddressProvider.getLocalAddress();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    this.port = server.getPort();

    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));

    // registration
    // invoke registration from the client side
    Configuration nameResolverConf = NameResolverConfiguration.CONF
        .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
        .set(NameResolverConfiguration.NAME_SERVICE_PORT, this.port)
        .set(NameResolverConfiguration.CACHE_TIMEOUT, this.TTL)
        .set(NameResolverConfiguration.RETRY_TIMEOUT, retryTimeout)
        .set(NameResolverConfiguration.RETRY_COUNT, retryCount)
        .build();

    final NameResolver client = Tang.Factory.getTang().newInjector(nameResolverConf).getInstance(NameClient.class);
    for (final Identifier id : idToAddrMap.keySet()) {
      client.register(id, idToAddrMap.get(id));
    }

    // wait
    final Set<Identifier> ids = idToAddrMap.keySet();
    busyWait(server, ids.size(), ids);

    // lookup
    final Identifier id1 = this.factory.getNewInstance("task1");
    final Identifier id2 = this.factory.getNewInstance("task2");

    final Map<Identifier, InetSocketAddress> respMap = new HashMap<Identifier, InetSocketAddress>();
    InetSocketAddress addr1 = client.lookup(id1);
    respMap.put(id1, addr1);
    InetSocketAddress addr2 = client.lookup(id2);
    respMap.put(id2, addr2);

    for (final Identifier id : respMap.keySet()) {
      LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[]{id, respMap.get(id)});
    }

    Assert.assertTrue(isEqual(idToAddrMap, respMap));

    // un-registration
    for (final Identifier id : idToAddrMap.keySet()) {
      client.unregister(id);
    }

    // wait
    busyWait(server, 0, ids);

    final Map<Identifier, InetSocketAddress> serverMap = new HashMap<Identifier, InetSocketAddress>();
    addr1 = server.lookup(id1);
    if (addr1 != null) serverMap.put(id1, addr1);
    addr2 = server.lookup(id1);
    if (addr2 != null) serverMap.put(id2, addr2);

    Assert.assertEquals(0, serverMap.size());

    client.close();
    server.close();
  }

  private boolean isEqual(final Map<Identifier, InetSocketAddress> map1,
                          final Map<Identifier, InetSocketAddress> map2) {

    if (map1.size() != map2.size()) {
      return false;
    }

    for (final Identifier id : map1.keySet()) {
      final InetSocketAddress addr1 = map1.get(id);
      final InetSocketAddress addr2 = map2.get(id);
      if (!addr1.equals(addr2)) {
        return false;
      }
    }

    return true;
  }

  private void busyWait(final NameServer server, final int expected, final Set<Identifier> ids) {
    int count = 0;
    for (; ; ) {
      final Iterable<NameAssignment> nas = server.lookup(ids);
      for (final @SuppressWarnings("unused") NameAssignment na : nas) {
        ++count;
      }
      if (count == expected) {
        break;
      }
      count = 0;
    }
  }
}