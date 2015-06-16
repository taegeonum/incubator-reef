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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.Optional;

/**
 * Default POJO implementation of ResourceStatusEvent.
 * Use newBuilder to construct an instance.
 */
public final class ResourceStatusEventImpl implements ResourceStatusEvent {
  private final String identifier;
  private final ReefServiceProtos.State state;
  private final Optional<String> diagnostics;
  private final Optional<Integer> exitCode;
  private final Optional<Boolean> isFromPreviousDriver;

  private ResourceStatusEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.state = BuilderUtils.notNull(builder.state);
    this.diagnostics = Optional.ofNullable(builder.diagnostics);
    this.exitCode = Optional.ofNullable(builder.exitCode);
    this.isFromPreviousDriver = Optional.ofNullable(builder.isFromPreviousDriver);
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public ReefServiceProtos.State getState() {
    return state;
  }

  @Override
  public Optional<String> getDiagnostics() {
    return diagnostics;
  }

  @Override
  public Optional<Integer> getExitCode() {
    return exitCode;
  }

  @Override
  public Optional<Boolean> getIsFromPreviousDriver() {
    return isFromPreviousDriver;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create ResourceStatusEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceStatusEvent> {
    private String identifier;
    private ReefServiceProtos.State state;
    private String diagnostics;
    private Integer exitCode;
    private Boolean isFromPreviousDriver;

    /**
     * @see ResourceStatusEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see ResourceStatusEvent#getState()
     */
    public Builder setState(final ReefServiceProtos.State state) {
      this.state = state;
      return this;
    }

    /**
     * @see ResourceStatusEvent#getDiagnostics()
     */
    public Builder setDiagnostics(final String diagnostics) {
      this.diagnostics = diagnostics;
      return this;
    }

    /**
     * @see ResourceStatusEvent#getExitCode()
     */
    public Builder setExitCode(final int exitCode) {
      this.exitCode = exitCode;
      return this;
    }

    /**
     * @see ResourceStatusEvent#getIsFromPreviousDriver()
     */
    public Builder setIsFromPreviousDriver(final boolean isFromPreviousDriver) {
      this.isFromPreviousDriver = isFromPreviousDriver;
      return this;
    }

    @Override
    public ResourceStatusEvent build() {
      return new ResourceStatusEventImpl(this);
    }
  }
}
