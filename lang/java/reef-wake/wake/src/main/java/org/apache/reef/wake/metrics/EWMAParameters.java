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
package org.apache.reef.wake.metrics;

import static java.lang.Math.exp;

/**
 * Default EWMA parameters.
 */
public final class EWMAParameters {
  public static final int INTERVAL = 5;
  public static final double SECONDS_PER_MINUTE = 60.0;
  public static final int ONE_MINUTE = 1;
  public static final double M1_ALPHA = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / ONE_MINUTE);
  public static final int FIVE_MINUTES = 5;
  public static final double M5_ALPHA = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / FIVE_MINUTES);
  public static final int FIFTEEN_MINUTES = 15;
  public static final double M15_ALPHA = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / FIFTEEN_MINUTES);

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private EWMAParameters() {
  }
}
