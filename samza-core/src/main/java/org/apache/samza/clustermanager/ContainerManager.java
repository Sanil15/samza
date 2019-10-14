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
import com.google.common.base.Preconditions;
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
 * Callbacks issued from  {@link ClusterResourceManager} aka {@link ContainerProcessManager> are intercepted
 * by ContainerManager to handle container failure and completions for both active and standby containers
 */
public class ContainerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);

  private static final Long DEFAULT_CONTROL_ACTION_EXPIRY = Duration.ofSeconds(20).toMillis();
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
    this.actions = new ConcurrentHashMap<>();
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
      handleContainerAllocationForExistingControlAction(request.getProcessorId(), allocatedResource, allocator, request, preferredHost);
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
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container was last deployed
   * @param exitStatus exit code returned by the container
   * @param preferredHostRetryDelay delay to be incurred before requesting resources
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerStop(String processorId, String containerId, String preferredHost, int exitStatus,
      Duration preferredHostRetryDelay, ContainerAllocator containerAllocator) {
    if (getMoveMetadata(processorId).isPresent()) {
      handleContainerStopForExistingControlAction(processorId, containerAllocator);
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
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerLaunchFail} to check behavior
   * Case 2. When standby is disabled the allocator issues a request for ANY_HOST resources
   *
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container is requested to be deployed
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerLaunchFail(String processorId, String containerId, String preferredHost,
      ContainerAllocator containerAllocator) {

    // TODO: if ControlAction is present and launch fail is on the destination host try to go back on source host, if that oo fails -> any host
    if (processorId !=null && getMoveMetadata(processorId).isPresent()) {
      containerAllocator.requestResource(processorId, getMoveMetadata(processorId).get().getSourceHost());
      // TODO: if you remove this here host affinity off might go to any_host which is fine but not for a cluster balancer
      markActionFailed(processorId);
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

  // ONLY AM RM CALLBACK

  /**
   *
   * @param processorId
   */
  void handleContainerLaunchSuccess(String processorId) {
    markActionComplete(processorId);
  }

  /**
   * Handles an expired resource request for both active and standby containers. Since a preferred host cannot be obtained
   * this method checks the availability of surplus ANY_HOST resources and launches the container if available. Otherwise
   * issues an ANY_HOST request. This behavior holds regardless of host-affinity enabled or not.
   *
   * @param processorId logical id of the container
   * @param preferredHost host on which container is requested to be deployed
   * @param request pending request for the preferred host
   * @param allocator allocator for requesting resources
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   */
  @VisibleForTesting
  void handleExpiredResourceRequest(String processorId, String preferredHost,
      SamzaResourceRequest request, ContainerAllocator allocator, ResourceRequestState resourceRequestState) {
    boolean resourceAvailableOnAnyHost = allocator.hasAllocatedResource(ResourceRequestState.ANY_HOST);

    // TODO: Check if existing ControlAction & request is made for stateless container, fail the request saying cannot
    // get requested resources

    if (getMoveMetadata(processorId).isPresent()) {
      LOG.info(
          "Move action expired since Cluster Manager was not able to allocate any resources for the container,  cancelling resource request & returning back");
      resourceRequestState.cancelResourceRequest(request);
      markActionFailed(processorId);
      return;
    }

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
  public void registerControlAction(String processorId, String destinationHost, ContainerAllocator containerAllocator, Optional<Long> requestExpiry) {
    LOG.info("ControlAction request to start container with processor id {} on host {}", processorId, destinationHost);
    Preconditions.checkNotNull(processorId, destinationHost);

    // Check if the container already has a control action pending
    if (actions.containsKey(processorId)) {
      logRejectedRequest(processorId, destinationHost,
          String.format("existing control action %s on container", actions.get(processorId)));
      return;
    }

    // Ignore the request the container is not running or is pending
    if (!samzaApplicationState.runningProcessors.containsKey(processorId)
        || samzaApplicationState.pendingProcessors.containsKey(processorId)) {
      logRejectedRequest(processorId, destinationHost, "processor is not running or is in pending state");
      return;
    }

    // TODO: Handle the restart request for the degraded state case where job is running without a container running

    SamzaResource currentResource = samzaApplicationState.runningProcessors.get(processorId);
    LOG.info("Processor ID: {} matched a active container with deployment ID: {} running on host: {}", processorId,
        currentResource.getContainerId(), currentResource.getHost());
    SamzaResourceRequest resourceRequest =
        new SamzaResourceRequest(currentResource.getNumCores(), currentResource.getMemoryMb(), destinationHost,
            processorId);

    // TODO: read expiry time from incoming requests
    ControlActionMetaData actionMetaData =
        new ControlActionMetaData(processorId, currentResource.getContainerId(), currentResource.getHost(),
            destinationHost, requestExpiry.isPresent() ? requestExpiry.get() : DEFAULT_CONTROL_ACTION_EXPIRY);

    // Record the resouce request for monitoring
    actionMetaData.recordResourceRequest(resourceRequest);
    actions.put(processorId, actionMetaData);
    // note this also updates state.preferredHost count
    containerAllocator.issueResourceRequest(resourceRequest);
    LOG.info("Registered a control action with metadata {} and issued a request for resources ", actionMetaData);
  }

  // Allocator thread only
  public boolean handleContainerAllocationForExistingControlAction(String processorId, SamzaResource resource, ContainerAllocator allocator, SamzaResourceRequest request, String preferredhost) {

    // What if you check here that the container has stopped? and if not just issue a stop, if stopped can another resource request be put for it
    // if a control action is already underway? -- see if you can eliminate isContainerShutdownRequested?

    // check if container is already dead without issuing a stop here, fail the move request
    if (samzaApplicationState.runningProcessors.containsKey(processorId)) {
      if (!getMoveMetadata(processorId).get().isContainerShutdownRequested()) {
        LOG.info("Requesting running container to shutdown due to existing control action {}",
            getMoveMetadata(processorId).get());

        clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
        synchronized (actions) {
          getMoveMetadata(processorId).get().setContainerShutdownRequested();
        }
      } else {
        Long times = System.currentTimeMillis();
        if (System.currentTimeMillis() - times > Duration.ofSeconds(5).toMillis()) {
          LOG.info("Waiting for the container with processor id:{} to Stop", processorId);
          times = System.currentTimeMillis();
        }
      }
    } else if(getMoveMetadata(processorId).get().getActiveConatainerStopped()){
      LOG.info("Active container is stopped issuing a run on the allocated resource");
      allocator.runStreamProcessor(request, preferredhost);
    } else { // TODO: check if the processor is pending
      //LOG.info("Did not find any running processors to stop");
    }


    // TODO: what is container is already stopped & launch on new host failed, try to put it on the last seen host if not that any host
    // Check if the container is not pending not running, but that won't be resilient
//    if (!getMoveMetadata(activeContainerProcessorID).get().isContainerShutdownRequested()) {
//      LOG.info("Move Request: Found an available container for Processor ID: {} on the preferred host: {}",
//          activeContainerProcessorID, resource.getHost());
//      LOG.info("Active Container Shutdown requested and failover is initiated");
//
//      // TODO: check if the container is stopped or container is running?
//
//      clusterResourceManager.stopStreamProcessor(
//          samzaApplicationState.runningProcessors.get(activeContainerProcessorID));
//      getMoveMetadata(activeContainerProcessorID).get().setContainerShutdownRequested();
//      return true;
//    }




// Not useful issues multiple stop requests

    //      if (samzaApplicationState.runningProcessors.containsKey(processorId)) {
//        LOG.info("Requesting running container to shutdown due to existing control action {}", getMoveMetadata(processorId).get());
//        clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
//      } else if (samzaApplicationState.pendingProcessors.containsKey(processorId)) {
//        // TODO: container is killed / stopped
//        LOG.info("Did not find the container is running state, it might be killed due to other reasons, cancelling the request");
//      } else {
//        LOG.info("Waiting for the container with processor id:{} to Stop", processorId);
//      }



    return false;
  }


  // AM RM callback thread, check for duplicate notifications
  public void handleContainerStopForExistingControlAction(String processorId, ContainerAllocator containerAllocator) {
      if (getMoveMetadata(processorId).isPresent()) {
        LOG.info("Setting the active container stopped to be true {}", processorId);
        getMoveMetadata(processorId).get().setActiveContainerStopped();

        // TODO: Do we need to recheck if we have preferred resource still & container shutdown is requested?
        // can the resource be stolen from here? Add the resource in the preferred resource queue
//        LOG.info("Issuing a run processor request for container with resource id {} for Control action {}", processorId,
//            actions.get(processorId));
//        if (containerAllocator.hasAllocatedResource(getMoveMetadata(processorId).get().getDestinationHost())) {
//
//          SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequest(processorId,
//              getMoveMetadata(processorId).get().getDestinationHost());
//          containerAllocator.runStreamProcessor(resourceRequest, resourceRequest.getPreferredHost());
//
//        } else {
//          // cancel any additional resource request for control action
//          // follow the general path for container stops to issue resquests on source host ?
//          // mark action failed
//          LOG.info("Resource got stolen attempt to go back to source host but that code does not exist");
//        }
      }
  }

  // TODO: or say this only works for host affinity enabled jobs, is there a reason to enabled host afiinity without any state
  // then you do not need this
  public boolean isResourceRequestedForControlAction(SamzaResource resource) {
    for (ControlActionMetaData metaData : actions.values()) {
      // these are the only two preferred host request made
      if (metaData.getSourceHost().equals(resource.getHost()) || metaData.getDestinationHost()
          .equals(resource.getHost())) {
        return true;
      }
    }
    return false;
  }

  // TODO: check concurrency
  public void markActionComplete(String processorId) {
    synchronized (actions) {
      LOG.info("Marking the action complete for Samza Container with Processor ID", processorId);
      this.actions.remove(processorId);
    }
  }

  public void markActionFailed(String processorId) {
    synchronized (actions) {
      LOG.info("Marking the action action failed for Samza Container with Processor ID", processorId);
      this.actions.remove(processorId);
    }
  }
  /**
   * Check if a activeContainerResource has failover-metadata associated with it
   */
  public Optional<ControlActionMetaData> getMoveMetadata(String processorId) {
    return this.actions.containsKey(processorId) ? Optional.of(this.actions.get(processorId)) : Optional.empty();
  }

  public Optional<Long> getActionExpiryTimeout(String processorId) {
    return this.actions.containsKey(processorId) ? Optional.of(
        this.actions.get(processorId).getRequestActionExpiryTimeout()) : Optional.empty();
  }

  private void logRejectedRequest(String activeContainerProcessorId, String destinationHost, String message) {
    LOG.info("Control action to start container with processor id {} on host {} is rejected due to {}",
        activeContainerProcessorId, destinationHost, message);
  }


}
