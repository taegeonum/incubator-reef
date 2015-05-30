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

import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.temp.Connection;
import org.apache.reef.io.network.temp.ConnectionPool;
import org.apache.reef.io.network.temp.NetworkService;
import org.apache.reef.io.network.temp.impl.DefaultNSExceptionHandler;
import org.apache.reef.io.network.temp.impl.EvaluatorNameClientProxy;
import org.apache.reef.io.network.temp.impl.NetworkEvent;
import org.apache.reef.io.network.temp.impl.NetworkServiceImpl;
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kgw on 2015. 5. 31..
 */
public class NetworkServiceTest {
  @Test
  public void networkServiceTest() throws Exception {
    IdentifierFactory factory = new StringIdentifierFactory();
    LocalAddressProvider localAddressProvider = LocalAddressProviderFactory.getInstance();
    NameServer server = new NameServerImpl(0, factory, localAddressProvider);
    int nameServerPort = server.getPort();
    String localAddress = localAddressProvider.getLocalAddress();
    EvaluatorNameClientProxy proxy = new EvaluatorNameClientProxy(
        localAddress,
        nameServerPort,
        factory,
        30000,
        5,
        1000,
        localAddressProvider
    );

    NetworkService a = new NetworkServiceImpl(factory, 0, new DefaultNSExceptionHandler()
        , new MessagingTransportFactory(), proxy);

    a.registerId("a");

    NetworkService b = new NetworkServiceImpl(factory, 0, new DefaultNSExceptionHandler()
        , new MessagingTransportFactory(), proxy);

    b.registerId("b");


    Identifier connectionId = factory.getNewInstance("connection");

    Object key = new Object();
    ConnectionPool<String> connPoolA = a.newConnectionPool(connectionId, new StringCodec(), new StringEventHandler(null));
    ConnectionPool<String> connPoolB = b.newConnectionPool(connectionId, new StringCodec(), new StringEventHandler(key));

    Connection<String> connAtoB = connPoolA.newConnection(b.getNetworkServiceId());
    connAtoB.open();
    for (int i = 0; i < 10 ; i++) {
      connAtoB.write("hihi");
    }
    synchronized(key) {
      key.wait();
    }

    System.out.println("Success");
    a.close();
    b.close();
  }

  class StringEventHandler implements EventHandler<NetworkEvent<String>> {
    private Object key;
    private AtomicInteger count = new AtomicInteger();
    StringEventHandler(Object key) {
      this.key = key;
    }
    @Override
    public void onNext(NetworkEvent<String> value) {
      System.out.println(value);
      if (count.incrementAndGet() == 10) {
        synchronized (key) {
          key.notify();
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    NetworkServiceTest t = new NetworkServiceTest();
    t.networkServiceTest();
  }
}
