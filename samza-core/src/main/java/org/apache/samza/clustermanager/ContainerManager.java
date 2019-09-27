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
package org.apache.samza.clustermanager;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ContainerManager is a single place that manages control actions like start, stop for both active and standby containers
 * ContainerManager acts as a brain for validating and issuing any actions on containers in the Job Coordinator.
 *
 * The requests to allocate resources resources made by {@link ContainerAllocator} can either expire or succeed.
 * When the requests succeeds the ContainerManager validates those requests before starting the container
 * When the requests expires the ContainerManager decides the next set of actions for the pending request.
 *
 * Callbacks issued from  {@link ClusterResourceManager} aka {@link ContainerProcessManager} are intercepted
 * by ContainerManager to handle container failure and completions for both active and standby containers
 */
public class ContainerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);

  /**
   * Resource-manager, used to stop containers
   */
  private ClusterResourceManager clusterResourceManager;
  private final SamzaApplicationState samzaApplicationState;

  private final ConcurrentHashMap<String, ControlActionMetaData> actions;

  private Optional<StandbyContainerManager> standbyContainerManager;

  public ContainerManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      Boolean standByEnabled) {
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    this.actions = new HashMap<>();
    // Enable standby container manager if required
    if (standByEnabled) {
      this.standbyContainerManager =
          Optional.of(new StandbyContainerManager(samzaApplicationState, clusterResourceManager));
    } else {
      this.standbyContainerManager = Optional.empty();
    }
  }

  /**
   * Handles the action to be taken after the container has been stopped.
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerStop} to check constraints.
   * Case 2. When standby is disabled there are two cases according to host-affinity being enabled
   *    Case 2.1. When host-affinity is enabled resources are requested on host where container was last seen
   *    Case 2.2. When host-affinity is disabled resources are requested for ANY_HOST
   *
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container was last deployed
   * @param exitStatus exit code returned by the container
   * @param preferredHostRetryDelay delay to be incurred before requesting resources
   * @param containerAllocator allocator for requesting resources
   */
  public void handleContainerStop(String processorId, String containerId, String preferredHost, int exitStatus,
      Duration preferredHostRetryDelay, ContainerAllocator containerAllocator) {
    if (actions.containsKey(processorId) && actions.get(preferredHost).isContainerShutdownRequested()) {
      containerAllocator.runStreamProcessor(actions.get(processorId).);
    } else if (standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .handleContainerStop(processorId, containerId, preferredHost, exitStatus, containerAllocator,
              preferredHostRetryDelay);
    } else {
      // If StandbyTasks are not enabled, we simply make a request for the preferredHost
      containerAllocator.requestResourceWithDelay(processorId, preferredHost, preferredHostRetryDelay);
    }
  }

  /**
   * Handle the container launch failure for active containers and standby (if enabled)
   *
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container is requested to be deployed
   * @param containerAllocator allocator for requesting resources
   */
  public void handleContainerLaunchFail(String processorId, String containerId, String preferredHost,
      ContainerAllocator containerAllocator) {
    if (processorId != null && standbyContainerManager.isPresent()) {
      standbyContainerManager.get().handleContainerLaunchFail(processorId, containerId, containerAllocator);
    } else if (processorId != null) {
      LOG.info("Falling back to ANY_HOST for Processor ID: {} since launch failed for Container ID: {} on host: {}",
          processorId, containerId, preferredHost);
      containerAllocator.requestResource(processorId, ResourceRequestState.ANY_HOST);
    } else {
      LOG.warn("Did not find a pending Processor ID for Container ID: {} on host: {}. "
          + "Ignoring invalid/redundant notification.", containerId, preferredHost);
    }
  }

  /**
   * Validate the container starts for both active and standby containers
   */
  public void handleResourceAllocationForContainer(SamzaResourceRequest request, String preferredHost, SamzaResource allocatedResource,
      ResourceRequestState resourceRequestState, ContainerAllocator allocator) {
    String activeContainerProcessorId = request.getProcessorId();
    // Check standby enabled case here
    if(actions.containsKey(activeContainerProcessorId)) {

      // if container is running or pending with combinations of move request or not
      // for running issue a stop
      // for already stopped do not do anything

      if(!actions.get(request.getProcessorId()).isContainerShutdownRequested()) {
        // what is the container failed out of natural exception so again validate container is running
        if (samzaApplicationState.runningProcessors.containsKey(activeContainerProcessorId)) {
          LOG.info("Found preferred resources for the control action {} requesting active container shutdown",
              actions.get(request.getProcessorId()));
          this.clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(activeContainerProcessorId));
          actions.get(activeContainerProcessorId).setContainerShutdownRequested();
        } else {
          // mark move failed ? since container is not running, it got stopped due to some other reason
        }
        // Active container shutdown already under progress
      }

    } else if (this.standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, allocatedResource, allocator,
              resourceRequestState);
    } else {
      allocator.runStreamProcessor(request, preferredHost);
    }
  }

  /**
   * Handles an expired resource request when {@code hostAffinityEnabled} is true, in this case since the
   * preferred host, we try to see if a surplus ANY_HOST is available in the request queue.
   */
  public void handleContainerRequestExpiredWhenHostAffinityEnabled(String processorId, String preferredHost,
      SamzaResourceRequest request, ContainerAllocator allocator, ResourceRequestState resourceRequestState) {
    boolean resourceAvailableOnAnyHost = allocator.hasAllocatedResource(ResourceRequestState.ANY_HOST);
    if (standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .handleExpiredResourceRequest(processorId, request,
              Optional.ofNullable(allocator.peekAllocatedResource(ResourceRequestState.ANY_HOST)), allocator,
              resourceRequestState);
    } else if (resourceAvailableOnAnyHost) {
      LOG.info("Request for Processor ID: {} on host: {} has expired. Running on ANY_HOST", processorId, preferredHost);
      allocator.runStreamProcessor(request, ResourceRequestState.ANY_HOST);
    } else {
      LOG.info("Request for Processor ID: {} on host: {} has expired. Requesting additional resources on ANY_HOST.",
          processorId, preferredHost);
      resourceRequestState.cancelResourceRequest(request);
      allocator.requestResource(processorId, ResourceRequestState.ANY_HOST);
    }
  }

  public synchronized void registerControlAction(String activeContainerProcessorID, String destinationHost, ContainerAllocator containerAllocator) {
    LOG.info("ControlAction request to start container with processor id {} on host {}", activeContainerProcessorID,
        destinationHost);
    Preconditions.checkNotNull(activeContainerProcessorID, destinationHost);
    Boolean validProcessorId = samzaApplicationState.runningProcessors.containsKey(activeContainerProcessorID) ||
        samzaApplicationState.pendingProcessors.containsKey(activeContainerProcessorID);

    // Check if the processor Id is invalid
    if (!validProcessorId) {
      logRejectedRequest(activeContainerProcessorID, destinationHost, "invalid processor id");
      return;
    }
    // Check if the container already has a control action pending pending
    if (actions.containsKey(activeContainerProcessorID)) {
      logRejectedRequest(activeContainerProcessorID, destinationHost,
          String.format("existing control action %s on container", actions.get(activeContainerProcessorID)));
      return;
    }
    // Container is in pending state
    if (samzaApplicationState.pendingProcessors.containsKey(activeContainerProcessorID)) {
      logRejectedRequest(activeContainerProcessorID, destinationHost, "container is in pending state");
    }

    SamzaResource currentResource = samzaApplicationState.runningProcessors.get(activeContainerProcessorID);
    LOG.info("Processor ID: {} matched a active container with deployment ID: {} running on host: {}",
        activeContainerProcessorID, currentResource.getContainerId(), currentResource.getHost());

    SamzaResourceRequest resourceRequest =
        new SamzaResourceRequest(currentResource.getNumCores(), currentResource.getMemoryMb(), destinationHost,
            activeContainerProcessorID);

    ControlActionMetaData actionMetaData =
        new ControlActionMetaData(activeContainerProcessorID, currentResource.getContainerId(), currentResource.getHost());
    // Update the state
    actionMetaData.recordResourceRequest(resourceRequest);
    actions.put(activeContainerProcessorID, actionMetaData);
    // note this also updates state.preferredHost count
    containerAllocator.issueResourceRequest(resourceRequest);
    LOG.info("Registered a control action with metadata {} and issed a request for resources ", actionMetaData);
  }

//
//  public synchronized boolean handleContainerAllocationForControlAction(String activeContainerProcessorID, String preferredHost) {
//    if (!getMoveMetadata(activeContainerProcessorID).get().isContainerShutdownRequested()) {
//      LOG.info("Move Request: Found an available container for Processor ID: {} on the preferred host: {}",
//          activeContainerProcessorID, preferredHost);
//      LOG.info("Active Container Shutdown requested and failover is initiated");
//      clusterResourceManager.stopStreamProcessor(
//          samzaApplicationState.runningProcessors.get(activeContainerProcessorID));
//      getMoveMetadata(activeContainerProcessorID).get().setContainerShutdownRequested();
//      return true;
//    }
//    LOG.info("Active Container Shutdown already Requested waiting for Container to Stop");
//    return false;
//  }

  public synchronized void markActionComplete(String activeContainerResourceID) {
    LOG.info("Marking the action complete for Samza Container with Processor ID", activeContainerResourceID);
    this.actions.remove(activeContainerResourceID);
  }

  public synchronized void markActionFailed(String activeContainerResourceID) {
    LOG.info("Marking the action action failed for Samza Container with Processor ID", activeContainerResourceID);
    this.actions.remove(activeContainerResourceID);
  }

  /**
   * Check if a activeContainerResource has failover-metadata associated with it
   */
  public Optional<ControlActionMetaData> getMoveMetadata(String activeContainerSamzaProcessorId) {
    return this.actions.containsKey(activeContainerSamzaProcessorId) ? Optional.of(
        this.actions.get(activeContainerSamzaProcessorId)) : Optional.empty();
  }

  private void logRejectedRequest(String activeContainerProcessorId, String destinationHost, String message) {
    LOG.info("Control action to start container with processor id {} on host {} is rejected due to {}",
        activeContainerProcessorId, destinationHost, message);
  }

}
