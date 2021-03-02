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

import com.pelion.connect.dm.utils.PelionConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class PelionSourceConnector extends SourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(PelionSourceConnector.class);

  private String connectName;

  private Map<String, String> configProps;

  private PelionSourceConnectorConfig config;

  @Override
  public String version() {
    return PelionConnectorUtils.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    connectName = props.get(PelionSourceConnectorConfig.CONNECT_NAME_CONFIG);
    LOG.info("Configuring connector : '{}'", connectName);
    try {
      config = new PelionSourceConnectorConfig(props);
      configProps = Collections.unmodifiableMap(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't init PelionSourceConnector "
          + "due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return PelionSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    LOG.info("Creating new tasks configs (maxTasks={})", maxTasks);

    // get the list of subscriptions
    final List<String> subscriptions = config.getList(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG);
    if (maxTasks > subscriptions.size()) {
      LOG.info("the number of subscriptions defined in the configuration file "
          + "is less than the maximum tasks number, only {} tasks "
          + "will be created", subscriptions.size());
      maxTasks = subscriptions.size();
    }

    // get the access keys for each task
    final List<String> accessKeys = config.getList(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG);
    if (maxTasks != accessKeys.size()) {
      throw new ConnectException("The list of access keys specified differs in size from the number of tasks");
    }

    final int numGroups = Math.min(subscriptions.size(), maxTasks);
    final List<List<String>> subscriptionsGrouped = ConnectorUtils
        .groupPartitions(subscriptions, numGroups);

    // we are now ready to create configs for each task
    final List<Map<String, String>> taskConfigs = new ArrayList<>(subscriptionsGrouped.size());

    final ListIterator<List<String>> iter = subscriptionsGrouped.listIterator();

    // Note: due to Pelion architecture, 'registration' messages get received
    // by all notification channels, so we should limit to one task to receive them
    boolean shouldPublishRegistrations = true;

    while (iter.hasNext()) {
      final int index = iter.nextIndex();
      final List<String> subs = iter.next();

      final Map<String, String> taskProps = new HashMap<>(configProps);
      // set the assigned subscriptions
      taskProps.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, String.join(",", subs));
      // set the assigned access-key
      taskProps.put(PelionSourceTaskConfig.PELION_TASK_ACCESS_KEY_CONFIG, accessKeys.get(index));

      // whether this task should publish registrations events
      taskProps.put(PelionSourceTaskConfig.PELION_TASK_PUBLISH_REGISTRATIONS, (shouldPublishRegistrations ? "true" : "false"));
      shouldPublishRegistrations = false; // no more for now on.

      taskConfigs.add(taskProps);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {
    LOG.info("Stopping connector : '{}'", connectName);
  }

  @Override
  public ConfigDef config() {
    return PelionSourceConnectorConfig.config();
  }
}
