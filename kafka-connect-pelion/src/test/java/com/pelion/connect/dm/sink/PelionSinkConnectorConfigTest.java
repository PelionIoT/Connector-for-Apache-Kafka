package com.pelion.connect.dm.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class PelionSinkConnectorConfigTest {

  private ConfigDef configDef = PelionSinkConnectorConfig.config();
  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(PelionSinkConnectorConfig.PELION_API_HOST_CONFIG, "api.us-east-1.mbedcloud.com");
    props.put(PelionSinkConnectorConfig.PELION_ACCESS_KEY_CONFIG, "key1, key2");
  }

  @Test
  public void doc() {
    System.out.println(PelionSinkConnectorConfig.config().toRst());
  }

  @Test(expected = ConfigException.class)
  public void shouldFailIfAccessKeyIsNotDefined() {
    new PelionSinkConnectorConfig(new HashMap<>());
  }

  @Test
  public void initialConfigIsValid() {
    assertTrue(configDef.validate(props)
        .stream()
        .allMatch(configValue -> configValue.errorMessages().size() == 0));
  }
}
