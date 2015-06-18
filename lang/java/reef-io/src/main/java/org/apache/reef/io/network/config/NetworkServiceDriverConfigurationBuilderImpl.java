package org.apache.reef.io.network.config;

import org.apache.reef.io.network.NetworkServiceDriverConfigurationBuilder;
import org.apache.reef.io.network.impl.DefaultNSLinkListener;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;


public final class NetworkServiceDriverConfigurationBuilderImpl implements NetworkServiceDriverConfigurationBuilder {


  private static NetworkServiceDriverConfigurationBuilder instance;
  private final NSConfigurationBuilderHelper helper;
  private static final String DRIVER_ID = "##DRIVER##";

  /**
   * NetworkServiceConfigurationBuilder for DriverSide
   * @param confSerializer
   */
  private NetworkServiceDriverConfigurationBuilderImpl(final ConfigurationSerializer confSerializer) {
    Configuration conf = NetworkServiceDriverConfiguration.CONF.build();
    this.helper = new NSConfigurationBuilderHelper(confSerializer, conf);
  }

  public static synchronized NetworkServiceDriverConfigurationBuilder getInstance() {
    if (instance == null) {
      instance = new NetworkServiceDriverConfigurationBuilderImpl(new AvroConfigurationSerializer());
    }
    return instance;
  }

  @Override
  public void setDriverConnectionFactory(final Class<? extends Name<String>> connectionFactoryId,
                                         final Class<? extends Codec<?>> codec,
                                         final Class<? extends EventHandler<?>> eventHandler) {
    this.helper.setConnectionFactory(DRIVER_ID, connectionFactoryId, codec, eventHandler, DefaultNSLinkListener.class);
  }

  @Override
  public void setDriverConnectionFactory(final Class<? extends Name<String>> connectionFactoryId,
                                         final Class<? extends Codec<?>> codec,
                                         final Class<? extends EventHandler<?>> eventHandler,
                                         final Class<? extends LinkListener<?>> linkListener) {
    this.helper.setConnectionFactory(DRIVER_ID, connectionFactoryId, codec, eventHandler, linkListener);
  }

  @Override
  public Configuration getDriverConfiguration(final Class<? extends Name<String>> connectionFactoryId) {
    return this.helper.getServiceConfiguration(DRIVER_ID, connectionFactoryId);
  }
}