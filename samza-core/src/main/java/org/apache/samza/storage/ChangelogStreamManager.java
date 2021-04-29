/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for creating the changelog stream. Used for reading, writing
 * and updating the task to changelog stream partition association in metadata store.
 */
public class ChangelogStreamManager {

  private static final Logger LOG = LoggerFactory.getLogger(ChangelogStreamManager.class);

  /**
   * Creates and validates the changelog streams of a samza job.
   *
   * @param config the configuration with changelog info.
   * @param maxChangeLogStreamPartitions the maximum number of changelog stream partitions to create.
   */
  public void createChangelogStreams(Config config, int maxChangeLogStreamPartitions) {
    // Get changelog store config
    StorageConfig storageConfig = new StorageConfig(config);
    ImmutableMap.Builder<String, SystemStream> storeNameSystemStreamMapBuilder = new ImmutableMap.Builder<>();
    storageConfig.getStoreNames().forEach(storeName -> {
      Optional<String> changelogStream = storageConfig.getChangelogStream(storeName);
      if (changelogStream.isPresent() && StringUtils.isNotBlank(changelogStream.get())) {
        storeNameSystemStreamMapBuilder.put(storeName, StreamUtil.getSystemStreamFromNames(changelogStream.get()));
      }
    });
    Map<String, SystemStream> storeNameSystemStreamMapping = storeNameSystemStreamMapBuilder.build();

    // Get SystemAdmin for changelog store's system and attempt to create the stream
    SystemConfig systemConfig = new SystemConfig(config);
    storeNameSystemStreamMapping.forEach((storeName, systemStream) -> {
      // Load system admin for this system.
      SystemAdmin systemAdmin = systemConfig
          .getSystemFactories()
          .get(systemStream.getSystem())
          .getAdmin(systemStream.getSystem(), config, ChangelogStreamManager.class.getSimpleName());

      if (systemAdmin == null) {
        throw new SamzaException(String.format(
            "Error creating changelog. Changelog on store %s uses system %s, which is missing from the configuration.",
            storeName, systemStream.getSystem()));
      }

      StreamSpec changelogSpec =
          StreamSpec.createChangeLogStreamSpec(systemStream.getStream(), systemStream.getSystem(),
              maxChangeLogStreamPartitions);

      systemAdmin.start();

      if (systemAdmin.createStream(changelogSpec)) {
        LOG.info(String.format("created changelog stream %s.", systemStream.getStream()));
      } else {
        LOG.info(String.format("changelog stream %s already exists.", systemStream.getStream()));
      }
      systemAdmin.validateStream(changelogSpec);

      if (storageConfig.getAccessLogEnabled(storeName)) {
        String accesslogStream = storageConfig.getAccessLogStream(systemStream.getStream());
        StreamSpec accesslogSpec =
            new StreamSpec(accesslogStream, accesslogStream, systemStream.getSystem(), maxChangeLogStreamPartitions);
        systemAdmin.createStream(accesslogSpec);
        systemAdmin.validateStream(accesslogSpec);
      }

      systemAdmin.stop();
    });
  }
}
