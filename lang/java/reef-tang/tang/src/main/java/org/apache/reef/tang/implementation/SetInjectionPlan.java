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
package org.apache.reef.tang.implementation;

import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.MonotonicHashSet;

import java.util.Set;

public class SetInjectionPlan<T> extends InjectionPlan<T> {
  private final Set<InjectionPlan<T>> entries = new MonotonicHashSet<>();
  private final int numAlternatives;
  private final boolean isAmbiguous;
  private final boolean isInjectable;

  public SetInjectionPlan(Node name, Set<InjectionPlan<T>> entries) {
    super(name);
    this.entries.addAll(entries);
    int numAlternatives = 1;
    boolean isAmbiguous = false;
    boolean isInjectable = true;
    for (InjectionPlan<T> ip : entries) {
      numAlternatives *= ip.getNumAlternatives();
      isAmbiguous |= ip.isAmbiguous();
      isInjectable &= ip.isInjectable();
    }
    this.numAlternatives = numAlternatives;
    this.isAmbiguous = isAmbiguous;
    this.isInjectable = isInjectable;
  }

  @Override
  public int getNumAlternatives() {
    return numAlternatives;
  }

  @Override
  public boolean isAmbiguous() {
    return isAmbiguous;
  }

  @Override
  public boolean isInjectable() {
    return isInjectable;
  }

  public Set<InjectionPlan<T>> getEntryPlans() {
    return new MonotonicHashSet<>(this.entries);
  }

  @Override
  protected String toAmbiguousInjectString() {
    StringBuilder sb = new StringBuilder(getNode().getFullName() + "(set) includes ambiguous plans [");
    for (InjectionPlan<T> ip : entries) {
      if (ip.isAmbiguous()) {
        sb.append("\n" + ip.toAmbiguousInjectString());
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  protected String toInfeasibleInjectString() {
    StringBuilder sb = new StringBuilder(getNode().getFullName() + "(set) includes infeasible plans [");
    for (InjectionPlan<T> ip : entries) {
      if (!ip.isFeasible()) {
        sb.append("\n" + ip.toInfeasibleInjectString());
      }
    }
    sb.append("\n]");
    return sb.toString();
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

  @Override
  public String toShallowString() {
    StringBuilder sb = new StringBuilder("set { ");
    for (InjectionPlan<T> ip : entries) {
      sb.append("\n" + ip.toShallowString());
    }
    sb.append("\n } ");
    return null;
  }

}
