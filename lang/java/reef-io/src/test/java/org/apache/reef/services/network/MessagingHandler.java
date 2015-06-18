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


import org.apache.reef.io.network.Message;
import org.apache.reef.services.network.util.Monitor;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MessagingHandler<T> implements EventHandler<Message<T>> {

  private static final Logger LOG = Logger.getLogger(MessagingHandler.class.getName());
  private final int expected;
  private final Monitor monitor;
  private AtomicInteger count = new AtomicInteger(0);

  @Inject
  public MessagingHandler( Monitor monitor, Integer expected) {
    this.monitor = monitor;
    this.expected = expected;
  }

  @Override
  public void onNext(Message<T> value) {
    count.incrementAndGet();
    LOG.log(Level.INFO,
        "OUT: {0} received {1} from {2} to {3}",
        new Object[]{value, value.getSrcId(), value.getDestId()});

    for (final T obj : value.getData()) {
      LOG.log(Level.INFO, "OUT: data: {0}", obj);
    }

    if (count.get() == expected) {
      monitor.mnotify();
    }
  }
}
