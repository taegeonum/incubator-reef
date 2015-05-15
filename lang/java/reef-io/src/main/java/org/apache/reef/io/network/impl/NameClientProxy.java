package org.apache.reef.io.network.impl;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.naming.NamingLookup;
import org.apache.reef.wake.Identifier;

import java.net.InetSocketAddress;

/**
 * Proxy registers and un-registers an identifier and can derive its address
 */
public interface NameClientProxy  extends NamingLookup, AutoCloseable {

  /**
   * NameClientProxy must collaborate with a name server and this method
   * returns the port number required to establish a connection.
   *
   * @return port number of name server
   */
  public int getNameServerPort();

  /**
   * Registers id
   *
   * @param id
   * @param address
   */
  public void registerId(Identifier id, InetSocketAddress address) throws NetworkException;

  /**
   * Unregisters id
   */
  public void unregisterId(Identifier id) throws NetworkException;
}
