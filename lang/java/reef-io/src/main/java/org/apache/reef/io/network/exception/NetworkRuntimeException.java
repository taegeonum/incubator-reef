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
package org.apache.reef.io.network.exception;

/**
 * Network service resourcemanager exception.
 */
public class NetworkRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new network resourcemanager exception with the specified detail message and cause.
   *
   * @param s the detailed message
   * @param e the cause
   */
  public NetworkRuntimeException(String s, Throwable e) {
    super(s, e);
  }

  /**
   * Constructs a new network resourcemanager exception with the specified detail message.
   *
   * @param s the detailed message
   */
  public NetworkRuntimeException(String s) {
    super(s);
  }

  /**
   * Constructs a new network resourcemanager exception with the specified cause.
   *
   * @param e the cause
   */
  public NetworkRuntimeException(Throwable e) {
    super(e);
  }
}
