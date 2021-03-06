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
package org.apache.reef.runtime.common.driver.catalog;

import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.junit.Assert;
import org.junit.Test;

public final class CatalogTest {


  /**
   * Basic catalog test that addes some nodes and checks
   * that they exist.
   */
  @Test
  public final void TestResourceCatalog() {
    final int nodes = 10;
    final ResourceCatalogImpl catalog = new ResourceCatalogImpl();

    for (int i = 0; i < nodes; i++) {
      catalog.handle(NodeDescriptorEventImpl.newBuilder()
          .setRackName("test-rack")
          .setHostName("test-" + i)
          .setPort(0)
          .setIdentifier("test-" + i)
          .setMemorySize(512)
          .build());
    }

    for (int i = 0; i < nodes; i++) {
      Assert.assertNotNull(catalog.getNode("test-" + i));
    }

    Assert.assertTrue(catalog.getRacks().size() == 1);

  }
}
