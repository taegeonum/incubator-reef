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
package org.apache.reef.io.network.temp;

import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.Set;

/**
 *
 */
public final class NetworkServiceParameters {

  @NamedParameter(doc = "port number of NetworkService", default_value = "0")
  public static final class Port implements Name<Integer> {

  }

  @NamedParameter(doc = "identifier factory of NetworkService", default_class = StringIdentifierFactory.class)
  public static final class IdentifierFactory implements Name<org.apache.reef.wake.IdentifierFactory> {

  }

  @NamedParameter
  public static final class NetworkServiceCodecs implements Name<Set<Codec<?>>> {

  }

  @NamedParameter
     public static final class NetworkServiceEventHandlers implements Name<Set<EventHandler<?>>> {

  }

  @NamedParameter
  public static final class NetworkServiceLinkListeners implements Name<Set<LinkListener<?>>> {

  }

  @NamedParameter(doc = "codec for a connection factory")
  public final class ConnectionFactoryCodecName implements Name<String> {

  }

  @NamedParameter(doc = "event handler for a connection factory")
  public class ConnectionFactoryHandlerName implements Name<String> {

  }

  @NamedParameter(doc = "connection factory id")
  public class ConnectionFactoryId implements Name<String> {

  }

  @NamedParameter(doc = "link listener for a connection factory")
  public class ConnectionFactoryListenerName implements Name<String> {

  }

  @NamedParameter(doc = "Serialized connection factory configurations")
  public final class SerializedConnFactoryConfigs implements Name<Set<String>> {

  }
}
