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

import com.pelion.connect.dm.utils.PelionAPI;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class PelionSourceConnectorConfig extends AbstractConfig {

  public static final String CONNECT_NAME_CONFIG = "name";

  public static final String PELION_API_HOST_CONFIG = "pelion.api.host";
  private static final String PELION_API_HOST_DOC = "The Pelion API host that this connector is bound to (defaults to 'api.us-east-1.mbedcloud.com').";

  public static final String PELION_ACCESS_KEY_LIST_CONFIG = "pelion.access.key.list";
  private static final String PELION_ACCESS_KEY_LIST_DOC = "A list of Pelion Access Keys be distributed to the tasks of this connector.";

  public static final String SUBSCRIPTIONS_GROUP = "Subscriptions";
  public static final String SUBSCRIPTIONS_CONFIG = "subscriptions";
  private static final String SUBSCRIPTIONS_DOC = "List of subscription aliases.";

  public PelionSourceConnectorConfig(final Map<?, ?> originals) {
    this(config(), originals);
  }

  PelionSourceConnectorConfig(final ConfigDef definition, final Map<?, ?> originals) {
    super(definition, originals, false);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(PELION_API_HOST_CONFIG, ConfigDef.Type.STRING, PelionAPI.DEFAULT_PELION_API_HOST,
            ConfigDef.Importance.MEDIUM, PELION_API_HOST_DOC)
        .define(PELION_ACCESS_KEY_LIST_CONFIG, ConfigDef.Type.LIST,
            ConfigDef.Importance.HIGH, PELION_ACCESS_KEY_LIST_DOC)
        .define(SUBSCRIPTIONS_CONFIG, ConfigDef.Type.LIST,
            ConfigDef.Importance.HIGH, SUBSCRIPTIONS_DOC,
            SUBSCRIPTIONS_GROUP, -1,
            ConfigDef.Width.NONE, SUBSCRIPTIONS_CONFIG);
  }
}
