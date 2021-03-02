/*
 * Copyright 2021 Pelion Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pelion.connect.dm.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class PelionSourceTaskConfig extends PelionSourceConnectorConfig {

  public static final String PELION_TASK_ACCESS_KEY_CONFIG = "pelion.access.key";
  private static final String PELION_TASK_ACCESS_KEY_DOC = "The Pelion Access Key bound to this task.";

  // internal config not used externally
  public static final String PELION_TASK_PUBLISH_REGISTRATIONS = "publish_registrations";
  private static final String PELION_TASK_PUBLISH_REGISTRATIONS_DOC = "Whether this task should publish registrations.";

  public static final String ENDPOINT_NAME_CONFIG = "endpoint-name";
  private static final String ENDPOINT_NAME_DOC = "Subscription rule based on endpoint name.";

  public static final String ENDPOINT_TYPE_CONFIG = "endpoint-type";
  private static final String ENDPOINT_TYPE_DOC = "Subscription rule based on endpoint type.";

  public static final String RESOURCE_PATH_CONFIG = "resource-path";
  private static final String RESOURCE_PATH_DOC = "Subscription rule based on resource paths.";

  private final EnrichedConnectorConfig enrichedConfig;

  protected static ConfigDef subscriptionConfig() {
    return new ConfigDef()
        .define(ENDPOINT_NAME_CONFIG, ConfigDef.Type.STRING, null,
            ConfigDef.Importance.HIGH, ENDPOINT_NAME_DOC)
        .define(ENDPOINT_TYPE_CONFIG, ConfigDef.Type.STRING, null,
            ConfigDef.Importance.HIGH, ENDPOINT_TYPE_DOC)
        .define(RESOURCE_PATH_CONFIG, ConfigDef.Type.LIST, null,
            ConfigDef.Importance.LOW, RESOURCE_PATH_DOC);
  }

  public PelionSourceTaskConfig(final Map<String, String> originals) {
    this(PelionSourceConnectorConfig.config(), originals);
  }

  PelionSourceTaskConfig(final ConfigDef configDef, final Map<String, String> originals) {
    super(configDef, originals);
    enrichedConfig = new EnrichedConnectorConfig(
        enrich(configDef, originals),
        originals
    );
  }

  @Override
  protected Object get(String key) {
    return enrichedConfig.get(key);
  }

  static ConfigDef enrich(final ConfigDef baseConfigDef, final Map<String, String> props) {
    final ConfigDef newDef = new ConfigDef(baseConfigDef);

    Object aliases = ConfigDef.parseType(SUBSCRIPTIONS_CONFIG,
        props.get(SUBSCRIPTIONS_CONFIG), ConfigDef.Type.LIST);
    LinkedHashSet<?> uniqAliases = new LinkedHashSet<>((List<?>) aliases);

    for (Object o : uniqAliases) {
      if (!(o instanceof String)) {
        throw new ConfigException("Item in " + aliases + " property is not of type string");
      }

      String alias = (String) o;
      final String prefix = SUBSCRIPTIONS_CONFIG + "." + alias + ".";
      final String group = SUBSCRIPTIONS_GROUP + ": " + alias;
      int orderInGroup = 0;

      newDef.embed(prefix, group, orderInGroup, subscriptionConfig());
    }

    // 'access-key' config is a top-level entry
    newDef.define(PELION_TASK_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD, null,
        ConfigDef.Importance.HIGH, PELION_TASK_ACCESS_KEY_DOC);
    // 'publish-registrations' config is a top-level entry
    newDef.define(PELION_TASK_PUBLISH_REGISTRATIONS, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.HIGH, PELION_TASK_PUBLISH_REGISTRATIONS_DOC);

    return newDef;
  }

  private static class EnrichedConnectorConfig extends AbstractConfig {

    EnrichedConnectorConfig(final ConfigDef configDef,
                            final Map<String, String> props) {
      super(configDef, props);
    }

    @Override
    public Object get(final String key) {
      return super.get(key);
    }
  }
}
