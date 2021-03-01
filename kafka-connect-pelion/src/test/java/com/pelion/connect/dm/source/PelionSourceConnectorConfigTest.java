package com.pelion.connect.dm.source;

import com.pelion.connect.dm.source.PelionSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class PelionSourceConnectorConfigTest {

  private ConfigDef configDef = PelionSourceConnectorConfig.config();
  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(PelionSourceConnectorConfig.PELION_API_HOST_CONFIG, "api.us-east-1.mbedcloud.com");
    props.put(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG, "key1, key2");
    props.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, "sub1, sub2");
  }

  @Test
  public void doc() {
    System.out.println(PelionSourceConnectorConfig.config().toRst());
  }

  @Test
  public void initialConfigIsValid() {
    assertTrue(configDef.validate(props)
        .stream()
        .allMatch(configValue -> configValue.errorMessages().size() == 0));
  }
}
