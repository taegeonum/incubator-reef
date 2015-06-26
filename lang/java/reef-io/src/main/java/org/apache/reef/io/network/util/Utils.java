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
package org.apache.reef.io.network.util;

import com.google.protobuf.ByteString;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.wake.ComparableIdentifier;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public final class Utils {

  private static final String DELIMITER = "-";

  /**
   * TODO: Merge with parseListCmp() into one generic implementation.
   */
  public static List<Identifier> parseList(
      final String ids, final IdentifierFactory factory) {
    final List<Identifier> result = new ArrayList<>();
    for (final String token : ids.split(DELIMITER)) {
      result.add(factory.getNewInstance(token.trim()));
    }
    return result;
  }

  /**
   * TODO: Merge with parseList() into one generic implementation.
   */
  public static List<ComparableIdentifier> parseListCmp(
      final String ids, final IdentifierFactory factory) {
    final List<ComparableIdentifier> result = new ArrayList<>();
    for (final String token : ids.split(DELIMITER)) {
      result.add((ComparableIdentifier) factory.getNewInstance(token.trim()));
    }
    return result;
  }

  public static String listToString(final List<ComparableIdentifier> ids) {
    return StringUtils.join(ids, DELIMITER);
  }

  public static List<Integer> createUniformCounts(final int elemSize, final int childSize) {
    final int remainder = elemSize % childSize;
    final int quotient = elemSize / childSize;
    final ArrayList<Integer> result = new ArrayList<>(childSize);
    result.addAll(Collections.nCopies(remainder, quotient + 1));
    result.addAll(Collections.nCopies(childSize - remainder, quotient));
    return Collections.unmodifiableList(result);
  }

  private static class AddressComparator implements Comparator<Inet4Address> {
    @Override
    public int compare(final Inet4Address aa, final Inet4Address ba) {
      final byte[] a = aa.getAddress();
      final byte[] b = ba.getAddress();
      // local subnet comes after all else.
      if (a[0] == 127 && b[0] != 127) {
        return 1;
      }
      if (a[0] != 127 && b[0] == 127) {
        return -1;
      }
      for (int i = 0; i < 4; i++) {
        if (a[i] < b[i]) {
          return -1;
        }
        if (a[i] > b[i]) {
          return 1;
        }
      }
      return 0;
    }
  }

  public static ReefNetworkGroupCommProtos.GroupCommMessage bldGCM(
      final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType,
      final Identifier from, final Identifier to, final byte[]... elements) {

    final ReefNetworkGroupCommProtos.GroupCommMessage.Builder GCMBuilder =
        ReefNetworkGroupCommProtos.GroupCommMessage.newBuilder()
            .setType(msgType)
            .setSrcid(from.toString())
            .setDestid(to.toString());

    final ReefNetworkGroupCommProtos.GroupMessageBody.Builder bodyBuilder =
        ReefNetworkGroupCommProtos.GroupMessageBody.newBuilder();

    for (final byte[] element : elements) {
      bodyBuilder.setData(ByteString.copyFrom(element));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }

    return GCMBuilder.build();
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Utils() {
  }
}
