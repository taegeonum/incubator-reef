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
package org.apache.reef.wake.test.remote;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;


public class TransportTest {
  private final LocalAddressProvider localAddressProvider;
  private final TransportFactory tpFactory;

  public TransportTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    this.tpFactory = injector.getInstance(TransportFactory.class);
  }

  final String logPrefix = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testTransportString() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final int expected = 2;
    final String hostAddress = this.localAddressProvider.getLocalAddress();
    ;
    final int port = 9100;

    // Codec<String>
    ReceiverStage<String> stage = new ReceiverStage<String>(new ObjectSerializableCodec<String>(), monitor, expected);
    Transport transport = tpFactory.newInstance(hostAddress, port, stage, stage, 1, 10000);

    // sending side
    Link<String> link = transport.open(
        new InetSocketAddress(hostAddress, port),
        new ObjectSerializableCodec<String>(),
        new LoggingLinkListener<String>());
    link.write(new String("hello1"));
    link.write(new String("hello2"));

    monitor.mwait();
    transport.close();
    timer.close();

    Assert.assertEquals(expected, stage.getCount());
  }

  @Test
  public void testTransportTestEvent() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final int expected = 2;
    final String hostAddress = this.localAddressProvider.getLocalAddress();
    final int port = 9100;

    // Codec<TestEvent>
    ReceiverStage<TestEvent> stage = new ReceiverStage<TestEvent>(new ObjectSerializableCodec<TestEvent>(), monitor, expected);
    Transport transport = tpFactory.newInstance(hostAddress, port, stage, stage, 1, 10000);

    // sending side
    Link<TestEvent> link = transport.open(
        new InetSocketAddress(hostAddress, port),
        new ObjectSerializableCodec<TestEvent>(),
        new LoggingLinkListener<TestEvent>());
    link.write(new TestEvent("hello1", 0.0));
    link.write(new TestEvent("hello2", 1.0));

    monitor.mwait();
    transport.close();
    timer.close();

    Assert.assertEquals(expected, stage.getCount());
  }

  class ReceiverStage<T> implements EStage<TransportEvent> {

    private final Codec<T> codec;
    private final Monitor monitor;
    private final int expected;
    private AtomicInteger count = new AtomicInteger(0);

    ReceiverStage(Codec<T> codec, Monitor monitor, int expected) {
      this.codec = codec;
      this.monitor = monitor;
      this.expected = expected;
    }

    int getCount() {
      return count.get();
    }

    @Override
    public void onNext(TransportEvent value) {
      codec.decode(value.getData());
      //System.out.println(value + " " + obj);      

      if (count.incrementAndGet() == expected)
        monitor.mnotify();
    }

    @Override
    public void close() throws Exception {
    }

  }

}
