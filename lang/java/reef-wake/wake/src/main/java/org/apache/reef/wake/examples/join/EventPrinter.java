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
package org.apache.reef.wake.examples.join;

import org.apache.reef.wake.rx.Observer;

public class EventPrinter<T> implements Observer<T> {

  @Override
  public void onNext(T value) {
    System.out.println(this + ": " + value);
  }

  @Override
  public void onError(Exception error) {
    System.err.println(this + ": " + error);
  }

  @Override
  public void onCompleted() {
    System.out.println(this + ": done!");
  }
}
