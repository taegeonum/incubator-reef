package org.apache.reef.io.network.temp;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.wake.Identifier;

import java.net.SocketAddress;
import java.util.List;

/**
 * Created by kgw on 2015. 5. 31..
 */
public interface Connection<T> extends AutoCloseable {

  public void open() throws NetworkException;

  public void write(List<T> messageList);

  public void write(T message);

  public SocketAddress getLocalAddress();

  public SocketAddress getRemoteAddress();

  public Identifier getRemoteId();

  public Identifier getConnectionId();

}
