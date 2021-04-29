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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
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

/**
 * Loads the managers responsible for the creation and loading of metadata related resources:
 * 1. Metadata streams: Changelog, Coordinator, Checkpoint
 * 2. Startpoints
 * 3. State backup: DaVinci, Ambry
 */
// TODO: Replace with a metadata admin interface when the {@link MetadataStore} is fully augmented to handle all metadata sources.
public class MetadataResourceAdmin {
  private final JobModel jobModel;
  private final Config config;

  private Optional<CheckpointManager> checkpointManager;
  private Optional<CoordinatorStreamManager> coordinatorStreamManager;
  private Optional<ChangelogStreamManager> changelogStreamManager;
  private Optional<StartpointManager> startpointManager;

  /**
   * Creates and loads the required metadata resources for checkpoints, changelog stream and other
   * resources related to the metadata system
   */
  MetadataResourceAdmin(JobModel jobModel, Config config, Optional<CheckpointManager> checkpointManager,
      Optional<CoordinatorStreamManager> coordinatorStreamManager,
      Optional<ChangelogStreamManager> changelogStreamManager, Optional<StartpointManager> startpointManager) {
    this.jobModel = jobModel;
    this.config = config;
    this.checkpointManager = checkpointManager;
    this.coordinatorStreamManager = coordinatorStreamManager;
    this.changelogStreamManager = changelogStreamManager;
    this.startpointManager = startpointManager;
  }

  public void createCheckpointMetadataResources() {
    checkpointManager.ifPresent(c -> c.createResources());
  }

  @VisibleForTesting
  public void createChangelogMetadataResouces() {
    changelogStreamManager.ifPresent(c -> c.createChangelogStreams(config, jobModel.maxChangeLogStreamPartitions));
  }

  // Remap changelog partitions to tasks for coordinator stream by calculating the job model again
  public void updateTaskToChangelogPartitionMappingOnRestart() {
    Map<TaskName, Integer> prevPartitionMappings = coordinatorStreamManager.get().readPartitionMapping();
    Map<TaskName, Integer> taskPartitionMappings = new HashMap<>();
    Map<String, ContainerModel> containers = jobModel.getContainers();
    for (ContainerModel containerModel : containers.values()) {
      for (TaskModel taskModel : containerModel.getTasks().values()) {
        taskPartitionMappings.put(taskModel.getTaskName(), taskModel.getChangelogPartition().getPartitionId());
      }
    }
    coordinatorStreamManager.get().updatePartitionMapping(prevPartitionMappings, taskPartitionMappings);
  }

  public void fanoutStartPoints() throws IOException {
    if (!startpointManager.isPresent()) {
      throw new SamzaException("Cannot fanout startpoints, StartpointManager not present");
    }
    startpointManager.get().start();
    try {
      startpointManager.get().fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
    } finally {
      startpointManager.get().stop();
    }
  }

  /**
   * PartitionMapping is needed to create the job model which is inturn needed to
   */
  public static Map<TaskName, Integer> readPartitionMapping(MetadataStore metadataStore) {
    return new CoordinatorStreamManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE)).readPartitionMapping();
  }

  @VisibleForTesting
  CheckpointManager getCheckpointManager() {
    return checkpointManager.get();
  }

  public static class Builder {
    private final JobModel jobModel;
    private final Config config;
    private final MetadataStore metadataStore;

    private Optional<CheckpointManager> checkpointManager = Optional.empty();
    private Optional<CoordinatorStreamManager> coordinatorStreamManager = Optional.empty();
    private Optional<ChangelogStreamManager> changelogStreamManager = Optional.empty();
    private Optional<StartpointManager> startpointManager = Optional.empty();

    public Builder(JobModel jobModel, MetadataStore metadataStore, Config config) {
      this.jobModel = Preconditions.checkNotNull(jobModel, "Job Model cannot be null");
      this.config = Preconditions.checkNotNull(config, "Config cannot be null");
      this.metadataStore = Preconditions.checkNotNull(metadataStore, "Metadata store cannot be null");
    }

    public Builder withCheckpointManager(MetricsRegistry metricsRegistry) {
      this.checkpointManager = this.checkpointManager =
          Optional.ofNullable(new TaskConfig(config).getCheckpointManager(metricsRegistry).orElse(null));
      return this;
    }

    public Builder withCoordinatorStreamManager() {
      this.coordinatorStreamManager = Optional.of(new CoordinatorStreamManager(
          new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE)));
      return this;
    }

    public Builder withChangelogStreamManager() {
      this.changelogStreamManager = Optional.of(new ChangelogStreamManager());
      return this;
    }

    public Builder withStartpointManager() {
      this.startpointManager = Optional.of(new StartpointManager(metadataStore));
      return this;
    }

    public MetadataResourceAdmin build() {
      return new MetadataResourceAdmin(jobModel, config, checkpointManager, coordinatorStreamManager,
          changelogStreamManager, startpointManager);
    }
  }
}
