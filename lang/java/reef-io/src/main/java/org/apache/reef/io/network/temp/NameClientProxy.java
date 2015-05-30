package org.apache.reef.io.network.temp;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.naming.NamingLookup;
import org.apache.reef.wake.Identifier;

import java.net.InetSocketAddress;

/**
 * Proxy registers and un-registers an identifier and can derive its address
 */
public interface NameClientProxy  extends NamingLookup, AutoCloseable {
  /**
   * Return the latest registered identifier through this proxy
   *
   * @return identifier of naming proxy
   */
  public Identifier getLocalIdentifier();

  /**
   * NameClientProxy must collaborate with a name server and this method
   * returns the port number required to establish a connection.
   *
   * @return port number of name server
   */
  public int getNameServerPort();

  /**
   * Registers local id
   *
   * @param id
   * @param address
   */
  public void registerId(Identifier id, InetSocketAddress address) throws NetworkException;

  /**
   * Unregisters local id
   */
  public void unregisterId() throws NetworkException;
}
