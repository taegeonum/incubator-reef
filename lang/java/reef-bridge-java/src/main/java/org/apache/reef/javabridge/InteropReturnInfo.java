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
package org.apache.reef.javabridge;

import java.util.ArrayList;

public class InteropReturnInfo {

  int returnCode;
  ArrayList<String> exceptionList = new ArrayList<String>();

  public void addExceptionString(String exceptionString) {
    exceptionList.add(exceptionString);
  }

  public boolean hasExceptions() {
    return !exceptionList.isEmpty();
  }

  public ArrayList<String> getExceptionList() {
    return exceptionList;
  }

  public int getReturnCode() {
    return returnCode;
  }

  public void setReturnCode(int rc) {
    returnCode = rc;
  }

  public void reset() {
    exceptionList = new ArrayList<String>();
    returnCode = 0;
  }
}
