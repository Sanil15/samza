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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ContainerManager is a centralized entity that manages control actions like start, stop for both active and standby containers
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

  private static final Long DEFAULT_CONTROL_ACTION_EXPIRY = Duration.ofSeconds(20).toMillis();
  /**
   * Resource-manager, used to stop containers
   */
  private final ClusterResourceManager clusterResourceManager;
  private final SamzaApplicationState samzaApplicationState;
  private final boolean hostAffinityEnabled;

  private final ConcurrentHashMap<String, ControlActionMetaData> actions;

  private Optional<StandbyContainerManager> standbyContainerManager;

  public ContainerManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      Boolean hostAffinityEnabled, Boolean standByEnabled) {
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    this.actions = new ConcurrentHashMap<>();
    this.hostAffinityEnabled = hostAffinityEnabled;
    // Enable standby container manager if required
    if (standByEnabled) {
      this.standbyContainerManager =
          Optional.of(new StandbyContainerManager(samzaApplicationState, clusterResourceManager));
    } else {
      this.standbyContainerManager = Optional.empty();
    }
  }

  /**
   * Handles the container start action for both active & standby containers.
   *
   * @param request pending request for the preferred host
   * @param preferredHost preferred host to start the container
   * @param allocatedResource resource allocated from {@link ClusterResourceManager}
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   * @param allocator to request resources from @{@link ClusterResourceManager}
   */
  void handleContainerLaunch(SamzaResourceRequest request, String preferredHost, SamzaResource allocatedResource,
      ResourceRequestState resourceRequestState, ContainerAllocator allocator) {
    // TODO: handle case of move action with standby
    if (getMoveMetadata(request.getProcessorId()).isPresent()) {
      // TODO: handle this when StandbyContainer is enabled & metadata request is present
      handleContainerAllocationForExistingControlAction(request.getProcessorId(), allocator, request, preferredHost);
    } else if (this.standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, allocatedResource, allocator,
              resourceRequestState);
    } else {
      allocator.runStreamProcessor(request, preferredHost);
    }
  }

  /**
   * Handles the action to be taken after the container has been stopped.
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerStop} to check constraints.
   * Case 2. When standby is disabled there are two cases according to host-affinity being enabled
   *    Case 2.1. When host-affinity is enabled resources are requested on host where container was last seen
   *    Case 2.2. When host-affinity is disabled resources are requested for ANY_HOST
   *
   * @param processorId logical id of the container eg 1,2,3
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container was last deployed
   * @param exitStatus exit code returned by the container
   * @param preferredHostRetryDelay delay to be incurred before requesting resources
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerStop(String processorId, String containerId, String preferredHost, int exitStatus,
      Duration preferredHostRetryDelay, ContainerAllocator containerAllocator) {
    if (getMoveMetadata(processorId).isPresent()) {
      LOG.info("Setting the container with processorId {} stopped to be true", processorId);
      getMoveMetadata(processorId).get().setActiveContainerStopped();
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
   * Handle the container launch failure for active containers and standby (if enabled).
   * Case 1. If this launch was issued due to an existing control action update the metadata to report failure
   * Case 2. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerLaunchFail} to check behavior
   * Case 3. When standby is disabled the allocator issues a request for ANY_HOST resources
   *
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container is requested to be deployed
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerLaunchFail(String processorId, String containerId, String preferredHost,
      ContainerAllocator containerAllocator) {
    if (processorId != null && getMoveMetadata(processorId).isPresent()) {
      ControlActionMetaData metaData = getMoveMetadata(processorId).get();
      // Issue a request to start the container on source host
      String sourceHost = hostAffinityEnabled ? ResourceRequestState.ANY_HOST : metaData.getSourceHost();
      containerAllocator.requestResource(processorId, sourceHost);
      metaData.setActionStatus(ControlActionStatus.StatusCode.FAILED, "failed to start container on destination host");
      LOG.info("Marking the control action failed with metadata {}", metaData);
      this.actions.remove(processorId);
    } else if (processorId != null && standbyContainerManager.isPresent()) {
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
   * Handles the state update on successful launch of a container, if this launch is due to a control action updates the
   * related metadata to report success
   *
   * @param processorId logical processor id of container 0,1,2
   */
  void handleContainerLaunchSuccess(String processorId) {
    Optional<ControlActionMetaData> metaData = getMoveMetadata(processorId);
    if (metaData.isPresent()) {
      metaData.get().setActionStatus(ControlActionStatus.StatusCode.SUCCEEDED, "Successfully completed the control action");
      LOG.info("Marking the control action sucesss {}", metaData);
      this.actions.remove(processorId);
    }
  }

  /**
   * Handles an expired resource request for both active and standby containers. Since a preferred host cannot be obtained
   * this method checks the availability of surplus ANY_HOST resources and launches the container if available. Otherwise
   * issues an ANY_HOST request. Only applies to HOST_AFFINITY enabled cases
   *
   * @param processorId logical id of the container
   * @param preferredHost host on which container is requested to be deployed
   * @param request pending request for the preferred host
   * @param allocator allocator for requesting resources
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   */
  @VisibleForTesting
  void handleExpiredRequestWithHostAffinityEnabled(String processorId, String preferredHost,
      SamzaResourceRequest request, ContainerAllocator allocator, ResourceRequestState resourceRequestState) {
    boolean resourceAvailableOnAnyHost = allocator.hasAllocatedResource(ResourceRequestState.ANY_HOST);

    if (getMoveMetadata(processorId).isPresent()) {
      LOG.info(
          "Move action expired since Cluster Manager was not able to allocate any resources for the container,  cancelling resource request & marking the action failed");
      resourceRequestState.cancelResourceRequest(request);
      getMoveMetadata(processorId).get()
          .setActionStatus(ControlActionStatus.StatusCode.FAILED,
              "failed the request because Cluster Manager did not return the requested resources");
      this.actions.remove(processorId);
      return;
    }

    if (!hostAffinityEnabled) {
      return;
    }

    // Only handle expired requests for host affinity enabled case
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

  // Always suppose to be invoked by the Kafka Consumer CPH thread, does this method need synchronized?

  /**
   * Registers a control action to move the running container to destination host, if destination host is same as the
   * host on which container is running, control action is treated as a restart.
   *
   * @param processorId logical id of the container 0, 1, 2
   * @param destinationHost host where container is desired to be moved
   * @param containerAllocator to request physical resources
   */
  public ControlActionStatus registerControlAction(String processorId, String destinationHost, ContainerAllocator containerAllocator, Optional<Long> requestExpiry) {
    LOG.info("Received ControlAction request to move or restart container with processor id {} to host {}", processorId, destinationHost);
    ControlActionStatus actionStatus = checkValidControlAction(processorId, destinationHost);
    if (actionStatus.status == ControlActionStatus.StatusCode.BAD_REQUEST) {
      return actionStatus;
    }
    // TODO: Handle the restart request for the degraded state case where job is running without a container running, read last seen host from locality
    SamzaResource currentResource = samzaApplicationState.runningProcessors.get(processorId);
    LOG.info("Processor ID: {} matched a active container with deployment ID: {} running on host: {}", processorId,
        currentResource.getContainerId(), currentResource.getHost());

    // TODO: Check if job needs support to move to Specific host when host affinity is enabled
    if (destinationHost.equals("ANY_HOST") || !hostAffinityEnabled) {
      LOG.info("Changing the requested host to {} because either it is requested or host affinity is disabled",
          ResourceRequestState.ANY_HOST);
      destinationHost = ResourceRequestState.ANY_HOST;
    }

    SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequest(processorId, destinationHost);
    ControlActionMetaData actionMetaData =
        new ControlActionMetaData(processorId, currentResource.getContainerId(), currentResource.getHost(),
            destinationHost, actionStatus, requestExpiry.isPresent() ? requestExpiry.get() : DEFAULT_CONTROL_ACTION_EXPIRY);

    // Record the resource request for monitoring
    actionMetaData.setActionStatus(ControlActionStatus.StatusCode.IN_PROGRESS);
    actionMetaData.recordResourceRequest(resourceRequest);
    actions.put(processorId, actionMetaData);
    // note this also updates state.preferredHost count
    containerAllocator.issueResourceRequest(resourceRequest);
    LOG.info("Control action with metadata {} and issued a request for resources in progress", actionMetaData);
    return actionStatus;
  }

  public Optional<Long> getActionExpiryTimeout(String processorId) {
    return this.actions.containsKey(processorId) ? Optional.of(
        this.actions.get(processorId).getRequestActionExpiryTimeout()) : Optional.empty();
  }

  /**
   * Handles the container allocation for an existing control action by issuing a stop on the active container and
   * waiting for the active container to shutdown. Once the active container shuts down then issue a start for the container
   * on the preferred host
   */
  private void handleContainerAllocationForExistingControlAction(String processorId, ContainerAllocator allocator,
      SamzaResourceRequest request, String preferredhost) {
    // check if container is already dead without issuing a stop here, fail the move request
    ControlActionMetaData actionMetaData = getMoveMetadata(processorId).get();
    if (samzaApplicationState.runningProcessors.containsKey(processorId)) {
      if (!actionMetaData.isContainerShutdownRequested()) {
        LOG.info("Requesting running container to shutdown due to existing control action {}",
            getMoveMetadata(processorId).get());
        clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
        actionMetaData.setContainerShutdownRequested();
      }
    } else if (actionMetaData.getActiveContainerStopped()) {
      LOG.info("Active container is stopped issuing a run on the allocated resource");
      allocator.runStreamProcessor(request, preferredhost);
    }
  }

  /**
   * Check if a activeContainerResource has control-action-metadata associated with it
   */
  private Optional<ControlActionMetaData> getMoveMetadata(String processorId) {
    return this.actions.containsKey(processorId) ? Optional.of(this.actions.get(processorId)) : Optional.empty();
  }

  /**
   * A valid control action is only issued for a running processor wiht a valid processor id
   */
  private ControlActionStatus checkValidControlAction(String processorId, String destinationHost) {
    String errorMessagePrefix =
        String.format("ControlAction to move or restart container with processor id %s to host %s is rejected due to",
            processorId, destinationHost);
    Boolean invalidAction = false;
    String errorMessage = null;
    if (processorId == null || destinationHost == null) {
      errorMessage = String.format("%s either processor id or the host argument is null", errorMessagePrefix);
      invalidAction = true;
    } else if (Integer.parseInt(processorId) >= samzaApplicationState.processorCount.get()) {
      errorMessage = String.format("%s invalid processor id", errorMessagePrefix);
      invalidAction = true;
    } else if (actions.containsKey(processorId)) {
      errorMessage = String.format("%s existing control action on container with metadata %s", errorMessagePrefix,
          actions.get(processorId));
      invalidAction = true;
    } else if (!samzaApplicationState.runningProcessors.containsKey(processorId)
        || samzaApplicationState.pendingProcessors.containsKey(processorId)) {
      errorMessage = String.format("%s container is either is not running or is in pending state", errorMessagePrefix);
      invalidAction = true;
    }

    if (invalidAction) {
      LOG.info(errorMessage);
      return new ControlActionStatus(ControlActionStatus.StatusCode.BAD_REQUEST, errorMessage);
    }

    return new ControlActionStatus(ControlActionStatus.StatusCode.ACCEPTED);
  }

}
