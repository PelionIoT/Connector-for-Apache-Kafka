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

package com.pelion.connect.dm.sink;

import com.pelion.connect.dm.utils.PelionAPI;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PelionSinkConnectorConfig extends AbstractConfig {

  public static final String CONNECT_NAME_CONFIG = "name";

  public static final String PELION_API_HOST_CONFIG = "pelion.api.host";
  private static final String PELION_API_HOST_DOC = "The Pelion API host that this connector is bound to (defaults to 'api.us-east-1.mbedcloud.com').";

  public static final String PELION_ACCESS_KEY_CONFIG = "pelion.access.key";
  private static final String PELION_ACCESS_KEY_DOC = "The Pelion Access Key bound to this task.";

  public static final String MAX_RETRIES = "max.retries";
  private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  public static final int RETRY_BACKOFF_MS_DEFAULT = Math.toIntExact(TimeUnit.SECONDS
      .toMillis(3L));
  private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";

  public static final String IGNORE_ERRORS = "ignore.errors";
  private static final String IGNORE_ERRORS_DOC = "Whether the sink connector should ignore device requests response errors and continue processing (default true).";

  public PelionSinkConnectorConfig(final Map<?, ?> originals) {
    this(config(), originals);
  }

  PelionSinkConnectorConfig(final ConfigDef definition, final Map<?, ?> originals) {
    super(definition, originals, false);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(PELION_API_HOST_CONFIG, ConfigDef.Type.STRING, PelionAPI.DEFAULT_PELION_API_HOST,
            ConfigDef.Importance.MEDIUM, PELION_API_HOST_DOC)
        .define(PELION_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD,
            ConfigDef.Importance.HIGH, PELION_ACCESS_KEY_DOC)
        .define(MAX_RETRIES, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
        .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC)
        .define(IGNORE_ERRORS, ConfigDef.Type.BOOLEAN, true,
            ConfigDef.Importance.MEDIUM, IGNORE_ERRORS_DOC);
  }
}
