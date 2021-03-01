package com.pelion.connect.dm.source;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class PelionSourceConnectorTest {

  private PelionSourceConnector connector;
  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG, "key1, key2, key3, key4, key5");
    props.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, "presub1, presub2, presub3, presub4, presub5");

    connector = new PelionSourceConnector();
  }

  @Test(expected = ConnectException.class)
  public void shouldCatchInvalidConfigs() {
    connector.start(new HashMap<>());
  }

  @Test
  public void shouldHonourTheNumberOfSubscriptionsIfMaxTasksIsGreater() {
    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(100);
    assertEquals(taskConfigs.size(), 5);
  }

  @Test(expected = ConnectException.class)
  public void shouldThrowExceptionIfMaxTasksDiffersFromTheNumberOfAccessKeys() {
    Map<String, String> propsInvalid = new HashMap<>();
    propsInvalid.put(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG, "key1, key2");
    propsInvalid.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, "presub1, presub2, presub3");
    connector.start(propsInvalid);
    connector.taskConfigs(3);
  }

  @Test
  public void shouldCorrectlyDistributeConfigsPerTask() {
    Map<String, String> props = new HashMap<>();
    props.put(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG, "key1, key2");
    props.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, "presub1, presub2, presub3, presub4, presub5");
    props.put("subscriptions.presub1.endpoint-name", "01767982c9250000000000010011579e");
    props.put("subscriptions.presub1.resource-path", "/3200/0/5501, /3201/0/5853");
    props.put("subscriptions.presub2.endpoint-type", "Light");
    props.put("subscriptions.presub2.resource-path", "sen/*");
    props.put("subscriptions.presub3.endpoint-type", "Sensor");
    props.put("subscriptions.presub4.resource-path", "/dev/temp, /dev/hum");
    props.put("subscriptions.presub5.endpoint-name", "0176c7561cf3000000000001001122d4");

    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
    assertEquals(2, taskConfigs.size());

    Map<String, String> task1Props = taskConfigs.get(0);
    PelionSourceTaskConfig task1Config = new PelionSourceTaskConfig(task1Props);
    assertEquals(3,
        task1Config.getList(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG).size());
    assertEquals(Arrays.asList("presub1", "presub2", "presub3"),
        task1Config.getList(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG));
    assertEquals("key1",
        task1Config.getPassword(PelionSourceTaskConfig.PELION_TASK_ACCESS_KEY_CONFIG).value());
    assertEquals(true,
        task1Config.getBoolean(PelionSourceTaskConfig.PELION_TASK_PUBLISH_REGISTRATIONS));

    // 'pre-sub1'
    assertEquals("01767982c9250000000000010011579e",
        task1Config.getString(String.format("subscriptions.%s.%s",
            "presub1",
            PelionSourceTaskConfig.ENDPOINT_NAME_CONFIG)));

    assertEquals(Arrays.asList("/3200/0/5501", "/3201/0/5853"),
        task1Config.getList(String.format("subscriptions.%s.%s",
            "presub1",
            PelionSourceTaskConfig.RESOURCE_PATH_CONFIG)));

    assertNull(task1Config.getString(String.format("subscriptions.%s.%s",
        "presub1",
        PelionSourceTaskConfig.ENDPOINT_TYPE_CONFIG)));

    // 'pre-sub2'
    assertNull(task1Config.getString(String.format("subscriptions.%s.%s",
        "presub2",
        PelionSourceTaskConfig.ENDPOINT_NAME_CONFIG)));

    assertEquals(Collections.singletonList("sen/*"),
        task1Config.getList(String.format("subscriptions.%s.%s",
            "presub2",
            PelionSourceTaskConfig.RESOURCE_PATH_CONFIG)));

    assertEquals("Light",
        task1Config.getString(String.format("subscriptions.%s.%s",
            "presub2",
            PelionSourceTaskConfig.ENDPOINT_TYPE_CONFIG)));

    // 'pre-sub3'
    assertNull(task1Config.getString(String.format("subscriptions.%s.%s",
        "presub3",
        PelionSourceTaskConfig.ENDPOINT_NAME_CONFIG)));

    assertNull(task1Config.getList(String.format("subscriptions.%s.%s",
        "presub3",
        PelionSourceTaskConfig.RESOURCE_PATH_CONFIG)));

    assertEquals("Sensor",
        task1Config.getString(String.format("subscriptions.%s.%s",
            "presub3",
            PelionSourceTaskConfig.ENDPOINT_TYPE_CONFIG)));

    Map<String, String> task2Props = taskConfigs.get(1);
    PelionSourceTaskConfig task2Config = new PelionSourceTaskConfig(task2Props);
    assertEquals(2,
        task2Config.getList(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG).size());
    assertEquals(Arrays.asList("presub4", "presub5"),
        task2Config.getList(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG));
    assertEquals("key2",
        task2Config.getPassword(PelionSourceTaskConfig.PELION_TASK_ACCESS_KEY_CONFIG).value());
    assertEquals(false,
        task2Config.getBoolean(PelionSourceTaskConfig.PELION_TASK_PUBLISH_REGISTRATIONS));

    // 'pre-sub4'
    assertNull(task2Config.getString(String.format("subscriptions.%s.%s",
        "presub4",
        PelionSourceTaskConfig.ENDPOINT_NAME_CONFIG)));

    assertEquals(Arrays.asList("/dev/temp", "/dev/hum"),
        task2Config.getList(String.format("subscriptions.%s.%s",
            "presub4",
            PelionSourceTaskConfig.RESOURCE_PATH_CONFIG)));

    assertNull(task2Config.getString(String.format("subscriptions.%s.%s",
        "presub4",
        PelionSourceTaskConfig.ENDPOINT_TYPE_CONFIG)));

    // 'pre-sub5'
    assertEquals("0176c7561cf3000000000001001122d4",
        task2Config.getString(String.format("subscriptions.%s.%s",
            "presub5",
            PelionSourceTaskConfig.ENDPOINT_NAME_CONFIG)));

    assertNull(task2Config.getList(String.format("subscriptions.%s.%s",
        "presub5",
        PelionSourceTaskConfig.RESOURCE_PATH_CONFIG)));

    assertNull(task2Config.getString(String.format("subscriptions.%s.%s",
        "presub5",
        PelionSourceTaskConfig.ENDPOINT_TYPE_CONFIG)));
  }

  @Test
  public void shouldNotHaveNullConfigDef() {
    assertNotNull(connector.config());
  }

  @Test
  public void shouldReturnConnectorType() {
    assertTrue(SourceConnector.class.isAssignableFrom(connector.getClass()));
  }

  @Test
  public void shouldReturnSourceTask() {
    assertEquals(PelionSourceTask.class, connector.taskClass());
  }

  @Test
  public void shouldStartAndStop() {
    connector.start(props);
    connector.stop();
  }

  @Test
  public void shouldValidateConfigs() {
    Config result = connector.validate(props);
    assertNotNull(result);
    result.configValues()
        .stream()
        .forEach(config -> assertTrue(config.errorMessages().isEmpty()));
  }

  @Test
  public void testVersion() {
    assertNotNull(connector.version());
    assertNotEquals("0.0.0.0", connector.version());
    assertNotEquals("unknown", connector.version());
    assertTrue(connector.version().matches("^(\\d+\\.){2}?(\\*|\\d+)(-.*)?$"));
  }
}
