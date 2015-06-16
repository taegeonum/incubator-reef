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
package org.apache.reef.wake.storage;

import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;

import java.io.FileInputStream;
import java.io.IOException;

public class SequentialFileReader implements EStage<ReadRequest> {
  final EventHandler<ReadResponse> dest = null;
  final FileHandlePool fdPool = new FileHandlePool();

  @Override
  public void onNext(ReadRequest value) {
    FileInputStream fin = fdPool.get(value.f);
    int readSoFar = 0;
    try {
      synchronized (fin) {
        fin.reset();
        fin.skip(value.offset);
        while (readSoFar != value.buf.length) {
          int ret = fin.read(value.buf, readSoFar, value.buf.length);
          if (ret == -1) {
            break;
          }
          readSoFar += ret;
        }
      }
    } catch (IOException e) {
      fdPool.release(value.f, fin);
//      err.onNext(null); //new ReadError(e));
    }
    fdPool.release(value.f, fin);
    dest.onNext(new ReadResponse(value.buf, readSoFar, value.id));
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

  }

}
