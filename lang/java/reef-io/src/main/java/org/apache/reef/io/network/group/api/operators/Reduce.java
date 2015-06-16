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
package org.apache.reef.io.network.group.api.operators;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.operators.ReduceReceiver;
import org.apache.reef.io.network.group.impl.operators.ReduceSender;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Identifier;

import java.util.List;

/**
 * MPI Reduce operator.
 * <p/>
 * This is another operator with root being receiver All senders send an element
 * to the receiver. These elements are passed through a reduce function and its
 * result is made available at the root
 */
public interface Reduce {

  /**
   * Receiver or Root.
   */
  @DefaultImplementation(ReduceReceiver.class)
  static interface Receiver<T> extends GroupCommOperator {

    /**
     * Receive values sent by senders and pass them through the reduce
     * function in default order.
     *
     * @return Result of applying reduce function on the elements gathered in default order.
     */
    T reduce() throws InterruptedException, NetworkException;

    /**
     * Receive values sent by senders and pass them through the reduce
     * function in specified order.
     *
     * @return Result of applying reduce function on the elements gathered in specified order.
     */
    T reduce(List<? extends Identifier> order) throws InterruptedException, NetworkException;

    /**
     * The reduce function to be applied on the set of received values.
     *
     * @return {@link ReduceFunction}
     */
    Reduce.ReduceFunction<T> getReduceFunction();
  }

  /**
   * Senders or non roots.
   */
  @DefaultImplementation(ReduceSender.class)
  static interface Sender<T> extends GroupCommOperator {

    /**
     * Send the element to the root.
     */
    void send(T element) throws NetworkException, InterruptedException;

    /**
     * The {@link ReduceFunction} to be applied on the set of received values.
     *
     * @return {@link ReduceFunction}
     */
    Reduce.ReduceFunction<T> getReduceFunction();
  }

  /**
   * Interface for a Reduce Function takes in an {@link Iterable} returns an.
   * aggregate value computed from the {@link Iterable}
   */
  static interface ReduceFunction<T> {
    /**
     * Apply the function on elements.
     *
     * @return aggregate value computed from elements.
     */
    T apply(Iterable<T> elements);
  }
}
