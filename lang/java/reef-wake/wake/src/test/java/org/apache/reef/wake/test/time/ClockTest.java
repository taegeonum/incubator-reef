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
package org.apache.reef.wake.test.time;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.LogicalTimer;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;

public class ClockTest {

  private static RuntimeClock buildClock() throws Exception {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang()
        .newConfigurationBuilder();

    final Injector injector = Tang.Factory.getTang()
        .newInjector(builder.build());

    return injector.getInstance(RuntimeClock.class);
  }

  private static RuntimeClock buildLogicalClock() throws Exception {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang()
        .newConfigurationBuilder();

    builder.bind(Timer.class, LogicalTimer.class);

    final Injector injector = Tang.Factory.getTang()
        .newInjector(builder.build());
    return injector.getInstance(RuntimeClock.class);
  }

  @Test
  public void testClock() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final RuntimeClock clock = buildClock();
    new Thread(clock).start();
    final RandomAlarmProducer alarmProducer = new RandomAlarmProducer(clock);
    ThreadPoolStage<Alarm> stage = new ThreadPoolStage<>(alarmProducer, 10);

    try {
      stage.onNext(null);
      Thread.sleep(5000);
      Assert.assertTrue(alarmProducer.getEventCount() > 40);
    } finally {
      stage.close();
      clock.close();
    }
  }

  @Test
  public void testAlarmRegistrationRaceConditions() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final RuntimeClock clock = buildClock();
    new Thread(clock).start();

    final EventRecorder earlierAlarmRecorder = new EventRecorder();
    final EventRecorder laterAlarmRecorder = new EventRecorder();

    try {
      // Schedule an Alarm that's far in the future
      clock.scheduleAlarm(5000, laterAlarmRecorder);
      Thread.sleep(1000);
      // By now, RuntimeClockImpl should be in a timed wait() for 5000 ms.
      // Scheduler an Alarm that should fire before the existing Alarm:
      clock.scheduleAlarm(2000, earlierAlarmRecorder);
      Thread.sleep(1000);
      // The earlier Alarm shouldn't have fired yet (we've only slept 1/2 time):
      Assert.assertEquals(0, earlierAlarmRecorder.events.size());
      Thread.sleep(1500);
      // The earlier Alarm should have fired, since 3500 > 2000 ms have passed:
      Assert.assertEquals(1, earlierAlarmRecorder.events.size());
      // And the later Alarm shouldn't have fired yet:
      Assert.assertEquals(0, laterAlarmRecorder.events.size());
      Thread.sleep(2000);
      // The later Alarm should have fired, since 5500 > 5000 ms have passed:
      Assert.assertEquals(1, laterAlarmRecorder.events.size());
    } finally {
      clock.close();
    }
  }

  @Test
  public void testMultipleCloseCalls() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final int numThreads = 3;
    final RuntimeClock clock = buildClock();
    new Thread(clock).start();
    final ThreadPoolStage<Alarm> stage = new ThreadPoolStage<>(new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm value) {
        clock.close();
      }
    }, numThreads);

    try {
      for (int i = 0; i < numThreads; ++i)
        stage.onNext(null);
      Thread.sleep(1000);
    } finally {
      stage.close();
      clock.close();
    }
  }

  @Test
  public void testSimultaneousAlarms() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final RuntimeClock clock = buildLogicalClock();
    new Thread(clock).start();

    final EventRecorder alarmRecorder = new EventRecorder();
    try {
      clock.scheduleAlarm(1000, alarmRecorder);
      clock.scheduleAlarm(1000, alarmRecorder);
      Thread.sleep(2000);
      Assert.assertEquals(2, alarmRecorder.events.size());
    } finally {
      clock.close();
    }
  }

  @Test
  public void testAlarmOrder() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final RuntimeClock clock = buildLogicalClock();
    new Thread(clock).start();

    final EventRecorder alarmRecorder = new EventRecorder();
    try {
      int numAlarms = 10;
      long[] expected = new long[numAlarms];
      for (int i = 0; i < numAlarms; ++i) {
        clock.scheduleAlarm(i * 100, alarmRecorder);
        expected[i] = i * 100;
      }

      Thread.sleep(2000);

      Long[] actualLong = new Long[numAlarms];
      alarmRecorder.timestamps.toArray(actualLong);
      long[] actual = new long[numAlarms];
      for (int i = 0; i < numAlarms; ++i) {
        actual[i] = actualLong[i];
      }
      Assert.assertArrayEquals(expected, actual);
    } finally {
      clock.close();
    }
  }

  /**
   * An EventHandler that records the events that it sees.
   */
  private static class EventRecorder implements EventHandler<Alarm> {

    /**
     * A synchronized List of the events recorded by this EventRecorder.
     */
    public final List<Time> events = Collections.synchronizedList(new ArrayList<Time>());
    public final List<Long> timestamps = Collections.synchronizedList(new ArrayList<Long>());

    @Override
    public void onNext(final Alarm event) {
      timestamps.add(event.getTimeStamp());
      events.add(event);
    }
  }

  private static class RandomAlarmProducer implements EventHandler<Alarm> {

    private final RuntimeClock clock;
    private final Random rand;
    private int eventCount = 0;

    public RandomAlarmProducer(RuntimeClock clock) {
      this.clock = clock;
      this.rand = new Random();
      this.eventCount = 0;
    }

    int getEventCount() {
      return eventCount;
    }

    @Override
    public void onNext(final Alarm value) {
      eventCount += 1;
      int duration = rand.nextInt(100) + 1;
      clock.scheduleAlarm(duration, this);
    }
  }
}
