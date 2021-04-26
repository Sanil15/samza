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

package org.apache.samza.coordinator;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import sun.jvm.hotspot.oops.Metadata;


/**
 * Loads the managers responsible for the creation and loading of metadata related resources.
 */
// TODO: Replace with a metadata admin interface when the {@link MetadataStore} is fully augmented to handle all metadata sources.
public class MetadataResourceUtil {
  private final CheckpointManager checkpointManager;
  private final Config config;
  private final JobModel jobModel; // TODO: Should be loaded by metadata store in the future
  private final CoordinatorStreamManager coordinatorStreamManager;
  private final MetadataStore metadataStore;

  /**
   * @param jobModel the loaded {@link JobModel}
   * @param metricsRegistry the registry for reporting metrics.
   */
  public MetadataResourceUtil(JobModel jobModel, MetadataStore metadataStore, MetricsRegistry metricsRegistry, Config config) {
    this.config = config;
    this.jobModel = jobModel;
    this.metadataStore = metadataStore;
    TaskConfig taskConfig = new TaskConfig(config);
    this.checkpointManager = taskConfig.getCheckpointManager(metricsRegistry).orElse(null);
    // build a JobModelManager and ChangelogStreamManager and perform partition assignments.
    this.coordinatorStreamManager = new CoordinatorStreamManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE));
  }

  /**
   * Creates and loads the required metadata resources for checkpoints, changelog stream and other
   * resources related to the metadata system
   */
  public void createResources() {
    if (checkpointManager != null) {
      checkpointManager.createResources();
    }
    createChangelogStreams();
  }

  // Remap changelog partitions to tasks for coordinator stream
  public void updateTaskToChangelogPartitionMapping() {
    Map<TaskName, Integer> prevPartitionMappings = coordinatorStreamManager.readPartitionMapping();
    Map<TaskName, Integer> taskPartitionMappings = new HashMap<>();
    Map<String, ContainerModel> containers = jobModel.getContainers();
    for (ContainerModel containerModel : containers.values()) {
      for (TaskModel taskModel : containerModel.getTasks().values()) {
        taskPartitionMappings.put(taskModel.getTaskName(), taskModel.getChangelogPartition().getPartitionId());
      }
    }
    coordinatorStreamManager.updatePartitionMapping(prevPartitionMappings, taskPartitionMappings);
  }

  public void fanoutStartPoints(boolean shouldFanOut) throws IOException {
    /*
     * We fanout startpoint if and only if
     *  1. Startpoint is enabled in configuration
     *  2. If AM HA is enabled, fanout only if startpoint enabled and job coordinator metadata changed
     */
    if (shouldFanOut) {
      StartpointManager startpointManager = new StartpointManager(metadataStore);
      startpointManager.start();
      try {
        startpointManager.fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
      } finally {
        startpointManager.stop();
      }
    }
  }


  public static Map<TaskName, Integer> readPartitionMapping(MetadataStore metadataStore) {
    return new CoordinatorStreamManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE)).readPartitionMapping();
  }

  @VisibleForTesting
  void createChangelogStreams() {
    ChangelogStreamManager.createChangelogStreams(config, jobModel.maxChangeLogStreamPartitions);
  }

  @VisibleForTesting
  CheckpointManager getCheckpointManager() {
    return checkpointManager;
  }

}
