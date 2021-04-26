package org.apache.samza.coordinator;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.storage.ChangelogStreamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorStreamManager {
  private static final Logger LOG = LoggerFactory.getLogger(ChangelogStreamManager.class);

  private final MetadataStore metadataStore;
  private final CoordinatorStreamValueSerde valueSerde;

  /**
   * Builds the CoordinatorStreamManager based upon the provided {@link MetadataStore} that is instantiated.
   * Setting up a metadata store instance is expensive which requires opening multiple connections
   * and reading tons of information. Fully instantiated metadata store is taken as a constructor argument
   * to reuse it across different utility classes. Uses the {@link CoordinatorStreamValueSerde} to serialize
   * messages before reading/writing into metadata store.
   *
   * @param metadataStore an instance of {@link MetadataStore} to read/write the container locality.
   */
  public CoordinatorStreamManager(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    this.valueSerde = new CoordinatorStreamValueSerde(SetChangelogMapping.TYPE);
  }


  /**
   * Reads the taskName to changelog partition assignments from the {@link MetadataStore}.
   *
   * @return TaskName to change LOG partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readPartitionMapping() {
    LOG.debug("Reading changelog partition information");
    final Map<TaskName, Integer> changelogMapping = new HashMap<>();
    metadataStore.all().forEach((taskName, partitionIdAsBytes) -> {
      String partitionId = valueSerde.fromBytes(partitionIdAsBytes);
      LOG.debug("TaskName: {} is mapped to {}", taskName, partitionId);
      if (StringUtils.isNotBlank(partitionId)) {
        changelogMapping.put(new TaskName(taskName), Integer.valueOf(partitionId));
      }
    });
    return changelogMapping;
  }

  /**
   * Writes the taskName to changelog partition assignments to the {@link MetadataStore}.
   * @param changelogEntries a map of the taskName to the changelog partition to be written to
   *                         metadata store.
   */
  public void writePartitionMapping(Map<TaskName, Integer> changelogEntries) {
    LOG.debug("Updating changelog information with: ");
    for (Map.Entry<TaskName, Integer> entry : changelogEntries.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      String taskName = entry.getKey().getTaskName();
      if (entry.getValue() != null) {
        String changeLogPartitionId = String.valueOf(entry.getValue());
        LOG.debug("TaskName: {} to Partition: {}", taskName, entry.getValue());
        metadataStore.put(taskName, valueSerde.toBytes(changeLogPartitionId));
      } else {
        LOG.debug("Deleting the TaskName: {}", taskName);
        metadataStore.delete(taskName);
      }
    }
    metadataStore.flush();
  }

  /**
   * Merges the previous and the new taskName to changelog partition mapping.
   * Writes the merged taskName to partition mapping to {@link MetadataStore}.
   * @param prevChangelogEntries The previous map of taskName to changelog partition.
   * @param newChangelogEntries The new map of taskName to changelog partition.
   */
  public void updatePartitionMapping(Map<TaskName, Integer> prevChangelogEntries,
      Map<TaskName, Integer> newChangelogEntries) {
    Map<TaskName, Integer> combinedEntries = new HashMap<>(newChangelogEntries);
    combinedEntries.putAll(prevChangelogEntries);
    writePartitionMapping(combinedEntries);
  }

}
