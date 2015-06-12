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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Tests for EvaluatorRequestorImpl.
 */
public class EvaluatorRequestorImplTest {
  private final ResourceCatalog resourceCatalog = mock(ResourceCatalog.class);
  private LoggingScopeFactory loggingScopeFactory;

  @Before
  public void setUp() throws InjectionException {
    loggingScopeFactory = Tang.Factory.getTang().newInjector().getInstance(LoggingScopeFactory.class);
  }

  /**
   * If only memory, no count is given, 1 evaluator should be requested.
   */
  @Test
  public void testMemoryOnly() {
    final int memory = 777;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler, loggingScopeFactory);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).build());
    Assert.assertEquals("Memory request did not make it", memory, requestHandler.get().getMemorySize().get().intValue());
    Assert.assertEquals("Number of requests did not make it", 1, requestHandler.get().getResourceCount());
  }

  /**
   * Checks whether memory and count make it correctly.
   */
  @Test
  public void testMemoryAndCount() {
    final int memory = 777;
    final int count = 9;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler, loggingScopeFactory);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).setNumber(count).build());
    Assert.assertEquals("Memory request did not make it", memory, requestHandler.get().getMemorySize().get().intValue());
    Assert.assertEquals("Number of requests did not make it", count, requestHandler.get().getResourceCount());
  }

  /**
   * Expect an IllegalArgumentException when a non-positive memory amount is passed.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalMemory() {
    final int memory = 0;
    final int count = 1;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler, loggingScopeFactory);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).setNumberOfCores(1).setNumber(count).build());
  }

  /**
   * Expect an IllegalArgumentException when a non-positive evaluator count is passed.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCount() {
    final int memory = 128;
    final int count = 0;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler, loggingScopeFactory);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).setNumberOfCores(1).setNumber(count).build());
  }

  private class DummyRequestHandler implements ResourceRequestHandler {
    private ResourceRequestEvent request;

    @Override
    public void onNext(final ResourceRequestEvent resourceRequestEvent) {
      this.request = resourceRequestEvent;
    }

    public ResourceRequestEvent get() {
      return this.request;
    }
  }
}
