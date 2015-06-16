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
package org.apache.reef.wake.test;

import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.*;


public class SyncStageTest {

  final String logPrefix = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testSyncStage() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());

    EStage<TestEventA> stage = new SyncStage<TestEventA>(new TestEventHandlerA(procSet));

    for (int i = 0; i < 10; ++i) {
      TestEventA a = new TestEventA();
      orgSet.add(a);

      stage.onNext(a);
    }

    stage.close();

    Assert.assertEquals(orgSet, procSet);
  }

  @Test
  public void testMultiSyncStage() throws Exception {
    System.out.println(name.getMethodName());

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());

    Map<Class<? extends TestEvent>, EventHandler<? extends TestEvent>> map
        = new HashMap<Class<? extends TestEvent>, EventHandler<? extends TestEvent>>();
    map.put(TestEventA.class, new TestEventHandlerA(procSet));
    map.put(TestEventB.class, new TestEventHandlerB(procSet));

    EventHandler<TestEvent> eventHandler = new MultiEventHandler<TestEvent>(map);

    EStage<TestEvent> stage = new SyncStage<TestEvent>(eventHandler);

    for (int i = 0; i < 10; ++i) {
      TestEventA a = new TestEventA();
      TestEventB b = new TestEventB();

      orgSet.add(a);
      orgSet.add(b);

      stage.onNext(a);
      stage.onNext(b);
    }

    stage.close();

    Assert.assertEquals(orgSet, procSet);
  }


  class TestEvent {
  }

  class TestEventA extends TestEvent {
  }

  class TestEventB extends TestEvent {
  }

  class TestEventHandlerA implements EventHandler<TestEventA> {

    private final Set<TestEvent> set;

    TestEventHandlerA(Set<TestEvent> set) {
      this.set = set;
    }

    public void onNext(TestEventA e) {
      set.add(e);
      System.out.println("TestEventHandlerA " + e);
    }
  }

  class TestEventHandlerB implements EventHandler<TestEventB> {

    private final Set<TestEvent> set;

    TestEventHandlerB(Set<TestEvent> set) {
      this.set = set;
    }

    public void onNext(TestEventB e) {
      set.add(e);
      System.out.println("TestEventHandlerB " + e);
    }
  }
}

