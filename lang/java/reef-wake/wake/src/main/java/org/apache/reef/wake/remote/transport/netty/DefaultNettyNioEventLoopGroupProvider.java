/**
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
package org.apache.reef.wake.remote.transport.netty;

import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.DefaultThreadFactory;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default NioEventLoopGroup provider
 */
public final class DefaultNettyNioEventLoopGroupProvider implements NettyNioEventLoopGroupProvider {

  private static final String CLASS_NAME = DefaultNettyNioEventLoopGroupProvider.class.getName();
  private static final Logger LOG = Logger.getLogger(CLASS_NAME);


  @NamedParameter(doc = "the number of thread of server boss NioEventGroup", default_value = "3")
  public class ServerBossThreadNumber implements Name<Integer> {
  }

  @NamedParameter(doc = "the number of thread of server worker NioEventGroup", default_value = "20")
  public class ServerWorkerThreadNumber implements Name<Integer> {
  }

  @NamedParameter(doc = "the number of thread of client worker NioEventGroup", default_value = "10")
  public class ClientWorkerThreadNumber implements Name<Integer> {
  }


  private final NioEventLoopGroup serverBossGroup;
  private final NioEventLoopGroup serverWorkerGroup;
  private final NioEventLoopGroup clientWorkerGroup;

  private AtomicBoolean closed;

  /*
   * @deprecated have an instance injected instead.
   */
  @Deprecated
  @Inject
  public DefaultNettyNioEventLoopGroupProvider(
      final @Parameter(ServerBossThreadNumber.class) int serverBossThreadNumber,
      final @Parameter(ServerWorkerThreadNumber.class) int serverWorkerThreadNumber,
      final @Parameter(ClientWorkerThreadNumber.class) int clientWorkerThreadNumber) {
    this.closed = new AtomicBoolean();
    this.serverBossGroup = new NioEventLoopGroup(serverBossThreadNumber, new DefaultThreadFactory(CLASS_NAME + "ServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(serverWorkerThreadNumber, new DefaultThreadFactory(CLASS_NAME + "ServerWorker"));
    this.clientWorkerGroup = new NioEventLoopGroup(clientWorkerThreadNumber, new DefaultThreadFactory(CLASS_NAME + "ClientWorker"));
    addShutdownHook();
  }

  /**
   * return NioEventLoopGroup for server boss
   *
   * @return serverBossGroup
   */
  public NioEventLoopGroup getServerBossGroup() {
    return serverBossGroup;
  }

  /**
   * return NioEventLoopGroup for server worker
   *
   * @return serverWorkerGroup
   */
  public NioEventLoopGroup getServerWorkerGroup() {
    return serverWorkerGroup;
  }

  /**
   * return NioEventLoopGroup for client worker
   *
   * @return clientWorkerGroup
   */
  public NioEventLoopGroup getClientWorkerGroup() {
    return clientWorkerGroup;
  }

  /**
   * close DefaultNettyNioEventLoopGroupProvider and release resources
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      LOG.log(Level.FINE, "Close DefaultNettyNioEventLoopGroupProvider");
      serverBossGroup.shutdownGracefully();
      serverWorkerGroup.shutdownGracefully();
      clientWorkerGroup.shutdownGracefully();
    } else {
      LOG.log(Level.WARNING, "The SharedNioEventLoopGroup had been closed");
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(
        new Thread(
          new Runnable() {
            @Override
            public void run() {
              close();
            }
         }
        )
    );
  }

}
