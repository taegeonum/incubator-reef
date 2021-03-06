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
package org.apache.reef.io.network.naming.serialization;

import org.apache.reef.wake.Identifier;

/**
 * Naming un-registration request.
 */
public class NamingUnregisterRequest extends NamingMessage {
  private final Identifier id;

  /**
   * Constructs a naming un-registration request.
   *
   * @param id the identifier
   */
  public NamingUnregisterRequest(Identifier id) {
    this.id = id;
  }

  /**
   * Gets an identifier.
   *
   * @return an identifier
   */
  public Identifier getIdentifier() {
    return id;
  }
}
