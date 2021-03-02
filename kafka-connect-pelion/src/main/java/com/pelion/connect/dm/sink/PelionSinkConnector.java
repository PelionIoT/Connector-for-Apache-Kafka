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

import com.pelion.connect.dm.utils.PelionConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PelionSinkConnector extends SinkConnector {

  private static final Logger LOG = LoggerFactory.getLogger(PelionSinkConnector.class);

  private String connectName;

  private Map<String, String> configProps;

  private PelionSinkTaskConfig config;

  @Override
  public String version() {
    return PelionConnectorUtils.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    connectName = props.get(PelionSinkConnectorConfig.CONNECT_NAME_CONFIG);
    LOG.info("Configuring connector : '{}'", connectName);
    try {
      config = new PelionSinkTaskConfig(props);
      configProps = Collections.unmodifiableMap(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't init PelionSinkConnector "
          + "due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return PelionSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    LOG.info("Creating new tasks configs (maxTasks={})", maxTasks);

    return Collections.nCopies(maxTasks, this.configProps);
  }

  @Override
  public void stop() {
    LOG.info("Stopping connector : '{}'", connectName);
  }

  @Override
  public ConfigDef config() {
    return PelionSinkConnectorConfig.config();
  }
}
