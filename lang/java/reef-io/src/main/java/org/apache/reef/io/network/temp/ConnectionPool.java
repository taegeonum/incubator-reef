package org.apache.reef.io.network.temp;

import org.apache.reef.io.network.Connection;
import org.apache.reef.wake.Identifier;

/**
 *
 */
public interface ConnectionPool<T> extends AutoCloseable {

  public Identifier getClientServiceId();

  public Connection<T> newConnection(Identifier remoteId);

  public Connection<T> getConnection(Identifier remoteId);

}
