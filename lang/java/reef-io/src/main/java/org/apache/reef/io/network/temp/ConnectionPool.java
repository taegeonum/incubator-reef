package org.apache.reef.io.network.temp;

import org.apache.reef.wake.Identifier;

/**
 *
 */
public interface ConnectionPool<T> extends AutoCloseable {

  public Identifier getConnectionId();

  public Connection<T> newConnection(Identifier remoteId);

  public Connection<T> getConnection(Identifier remoteId);

}
