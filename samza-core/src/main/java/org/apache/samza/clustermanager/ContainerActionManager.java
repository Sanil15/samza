package org.apache.samza.clustermanager;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ContainerActionManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerActionManager.class);
  private final SamzaApplicationState samzaApplicationState;
  // Resource-manager, used to stop containers
  private ClusterResourceManager clusterResourceManager;

  private final ActiveContainerController activeContainerController;
  private final Optional<StandbyContainerController> standbyContainerController;


  public ContainerActionManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager, Boolean standByEnabled) {
   this.samzaApplicationState = samzaApplicationState;
   this.clusterResourceManager = clusterResourceManager;
   this.activeContainerController = new ActiveContainerController();
   if (standByEnabled) {
     standbyContainerController = Optional.of(new StandbyContainerController())
   } else {
     standbyContainerController = Optional.empty();
   }
  }

  enum ContainerAction {
    MOVE, RESTART
  }

  public class ActiveContainerController {
    /*
     * Update the state in case of failovers, containers stopped etc
     */

    private final Map<String, ControlActionMetaData> actions;

    ActiveContainerController() {
      this.actions = new HashMap<>();
    }

    public synchronized void registerAction(String activeContainerID, String activeContainerSamzaProcessorID, String currentHost, SamzaResourceRequest resourceRequest) {
      // this active container's resource ID is already registered, in which case update the metadata
      if (actions.containsKey(activeContainerSamzaProcessorID)) {
        LOG.info("Already a failover is requested: {}, can't accept another one", actions.get(activeContainerSamzaProcessorID));
      } else {
        ControlActionMetaData controlActionMetaData =
            new ControlActionMetaData(activeContainerID, activeContainerSamzaProcessorID, currentHost, resourceRequest);
        LOG.info("ContainerMoveAction Registering a new request failover with metadata {}", controlActionMetaData);
        this.actions.put(activeContainerSamzaProcessorID, controlActionMetaData);
      }
    }

    public synchronized boolean initiaiteFailover(String activeContainerProcessorID, String preferredHost) {
      if (!getMoveMetadata(activeContainerProcessorID).get().isContainerShutdownRequested()) {
        LOG.info("Move Request: Found an available container for Processor ID: {} on the preferred host: {}", activeContainerProcessorID, preferredHost);
        LOG.info("Active Container Shutdown requested and failover is initiated");
        clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(activeContainerProcessorID));
        getMoveMetadata(activeContainerProcessorID).get().setContainerShutdownRequested();
        return true;
      }
      LOG.info("Active Container Shutdown already Requested waiting for Container to Stop");
      return false;
    }

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
      return this.actions.containsKey(activeContainerSamzaProcessorId) ? Optional.of(this.actions.get(activeContainerSamzaProcessorId)) : Optional.empty();
    }


  }

  public class StandbyContainerController {
    // Map of samza containerIDs to their corresponding active and standby containers, e.g., 0 -> {0-0, 0-1}, 0-0 -> {0, 0-1}
    // This is used for checking no two standbys or active-standby-pair are started on the same host
    private final Map<String, List<String>> standbyContainerConstraints;

    // Map of active containers that are in failover, indexed by the active container's resourceID (at the time of failure)
    private final Map<String, FailoverMetadata> failovers;

    public StandbyContainerController() {
      this.failovers = new ConcurrentHashMap<>();
      this.standbyContainerConstraints = new HashMap<>();
      JobModel jobModel = samzaApplicationState.jobModelManager.jobModel();

      // populate the standbyContainerConstraints map by iterating over all containers
      jobModel.getContainers()
          .keySet()
          .forEach(containerId -> standbyContainerConstraints.put(containerId,
              StandbyTaskUtil.getStandbyContainerConstraints(containerId, jobModel)));

      LOG.info("Populated standbyContainerConstraints map {}", standbyContainerConstraints);
    }

    /**
     * We handle the stopping of a container depending on the case which is decided using the exit-status:
     *    Case 1. an Active-Container which has stopped for an "unknown" reason, then we start it on the given preferredHost (but we record the resource-request)
     *    Case 2. Active container has stopped because of node failure, thene we initiate a failover
     *    Case 3. StandbyContainer has stopped after it was chosen for failover, see {@link StandbyContainerController#handleStandbyContainerStop}
     *    Case 4. StandbyContainer has stopped but not because of a failover, see {@link StandbyContainerController#handleStandbyContainerStop}
     *
     * @param containerID containerID of the stopped container
     * @param resourceID last resourceID of the stopped container
     * @param preferredHost the host on which the container was running
     * @param exitStatus the exit status of the failed container
     * @param containerAllocator the container allocator
     */
    public void handleContainerStop(String containerID, String resourceID, String preferredHost, int exitStatus,
        AbstractContainerAllocator containerAllocator, Duration preferredHostRetryDelay) {

      if (StandbyTaskUtil.isStandbyContainer(containerID)) {
        handleStandbyContainerStop(containerID, resourceID, preferredHost, containerAllocator, preferredHostRetryDelay);
      } else {
        // initiate failover for the active container based on the exitStatus
        switch (exitStatus) {
          case SamzaResourceStatus.DISK_FAIL:
          case SamzaResourceStatus.ABORTED:
          case SamzaResourceStatus.PREEMPTED:
            initiateStandbyAwareAllocation(containerID, resourceID, containerAllocator);
            break;
          // in all other cases, request-resource for the failed container, but record the resource-request, so that
          // if this request expires, we can do a failover -- select a standby to stop & start the active on standby's host
          default:
            LOG.info("Requesting resource for active-container {} on host {}", containerID, preferredHost);
            SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequestWithDelay(containerID, preferredHost, preferredHostRetryDelay);

            FailoverMetadata failoverMetadata = registerActiveContainerFailure(containerID, resourceID);
            failoverMetadata.recordResourceRequest(resourceRequest);
            containerAllocator.issueResourceRequest(resourceRequest);
            break;
        }
      }
    }

    /**
     * Handle the failed launch of a container, based on
     *    Case 1. If it is an active container, then initiate a failover for it.
     *    Case 2. If it is standby container, request a new resource on AnyHost.
     * @param containerID the ID of the container that has failed
     */
    public void handleContainerLaunchFail(String containerID, String resourceID,
        AbstractContainerAllocator containerAllocator) {

      if (StandbyTaskUtil.isStandbyContainer(containerID)) {
        LOG.info("Handling launch fail for standby-container {}, requesting resource on any host {}", containerID);
        containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
      } else {
        initiateStandbyAwareAllocation(containerID, resourceID, containerAllocator);
      }
    }

    /**
     *  If a standby container has stopped, then there are two possible cases
     *    Case 1. during a failover, the standby container was stopped for an active's start, then we
     *       1. request a resource on the standby's host to place the activeContainer, and
     *       2. request anyhost to place this standby
     *
     *    Case 2. independent of a failover, the standby container stopped, in which proceed with its resource-request
     * @param standbyContainerID SamzaContainerID of the standby container
     * @param preferredHost Preferred host of the standby container
     */
    private void handleStandbyContainerStop(String standbyContainerID, String resourceID, String preferredHost,
        AbstractContainerAllocator containerAllocator, Duration preferredHostRetryDelay) {

      // if this standbyContainerResource was stopped for a failover, we will find a metadata entry
      Optional<FailoverMetadata> failoverMetadata = this.checkIfUsedForFailover(resourceID);

      if (failoverMetadata.isPresent()) {
        String activeContainerID = failoverMetadata.get().activeContainerID;
        String standbyContainerHostname = failoverMetadata.get().getStandbyContainerHostname(resourceID);

        LOG.info("Requesting resource for active container {} on host {}, and backup container {} on any host",
            activeContainerID, standbyContainerHostname, standbyContainerID);

        // request standbycontainer's host for active-container
        SamzaResourceRequest resourceRequestForActive =
            containerAllocator.getResourceRequestWithDelay(activeContainerID, standbyContainerHostname, preferredHostRetryDelay);
        // record the resource request, before issuing it to avoid race with allocation-thread
        failoverMetadata.get().recordResourceRequest(resourceRequestForActive);
        containerAllocator.issueResourceRequest(resourceRequestForActive);

        // request any-host for standby container
        containerAllocator.requestResource(standbyContainerID, ResourceRequestState.ANY_HOST);
      } else {
        LOG.info("Issuing request for standby container {} on host {}, since this is not for a failover",
            standbyContainerID, preferredHost);
        containerAllocator.requestResourceWithDelay(standbyContainerID, preferredHost, preferredHostRetryDelay);
      }
    }

    /** Method to handle standby-aware allocation for an active container.
     *  We try to find a standby host for the active container, and issue a stop on any standby-containers running on it,
     *  request resource to place the active on the standby's host, and one to place the standby elsewhere.
     *
     * @param activeContainerID the samzaContainerID of the active-container
     * @param resourceID  the samza-resource-ID of the container when it failed (used to index failover-state)
     */
    private void initiateStandbyAwareAllocation(String activeContainerID, String resourceID,
        AbstractContainerAllocator containerAllocator) {

      String standbyHost = this.selectStandbyHost(activeContainerID, resourceID);

      // if the standbyHost returned is anyhost, we proceed with the request directly
      if (standbyHost.equals(ResourceRequestState.ANY_HOST)) {
        LOG.info("No standby container found for active container {}, making a resource-request for placing {} on {}, active's resourceID: {}",
            activeContainerID, activeContainerID, ResourceRequestState.ANY_HOST, resourceID);
        samzaApplicationState.failoversToAnyHost.incrementAndGet();
        containerAllocator.requestResource(activeContainerID, ResourceRequestState.ANY_HOST);

      } else {

        // Check if there is a running standby-container on that host that needs to be stopped
        List<String> standbySamzaContainerIds = this.standbyContainerConstraints.get(activeContainerID);

        Map<String, SamzaResource> runningStandbyContainersOnHost = new HashMap<>();
        samzaApplicationState.runningProcessors.forEach((samzaContainerId, samzaResource) -> {
          if (standbySamzaContainerIds.contains(samzaContainerId) && samzaResource.getHost().equals(standbyHost)) {
            runningStandbyContainersOnHost.put(samzaContainerId, samzaResource);
          }
        });

        if (runningStandbyContainersOnHost.isEmpty()) {
          // if there are no running standby-containers on the standbyHost, we proceed to directly make a resource request

          LOG.info("No running standby container to stop on host {}, making a resource-request for placing {} on {}, active's resourceID: {}",
              standbyHost, activeContainerID, standbyHost, resourceID);
          FailoverMetadata failoverMetadata = this.registerActiveContainerFailure(activeContainerID, resourceID);

          // record the resource request, before issuing it to avoid race with allocation-thread
          SamzaResourceRequest resourceRequestForActive =
              containerAllocator.getResourceRequest(activeContainerID, standbyHost);
          failoverMetadata.recordResourceRequest(resourceRequestForActive);
          containerAllocator.issueResourceRequest(resourceRequestForActive);
          samzaApplicationState.failoversToStandby.incrementAndGet();
        } else {
          // if there is a running standby-container on the standbyHost, we issue a stop (the stopComplete callback completes the remainder of the flow)
          FailoverMetadata failoverMetadata = this.registerActiveContainerFailure(activeContainerID, resourceID);

          runningStandbyContainersOnHost.forEach((standbyContainerID, standbyResource) -> {
            LOG.info("Initiating failover and stopping standby container, found standbyContainer {} = resource {}, "
                    + "for active container {}", runningStandbyContainersOnHost.keySet(),
                runningStandbyContainersOnHost.values(), activeContainerID);
            failoverMetadata.updateStandbyContainer(standbyResource.getContainerId(), standbyResource.getHost());
            samzaApplicationState.failoversToStandby.incrementAndGet();
            clusterResourceManager.stopStreamProcessor(standbyResource);
          });

          // if multiple standbys are on the same host, we are in an invalid state, so we fail the deploy and retry
          if (runningStandbyContainersOnHost.size() > 1) {
            throw new SamzaException(
                "Invalid State. Multiple standby containers found running on one host:" + runningStandbyContainersOnHost);
          }
        }
      }
    }

    /**
     * Method to select a standby host for a given active container.
     * 1. We first try to select a host which has a running standby-container, that we haven't already selected for failover.
     * 2. If we dont any such host, we iterate over last-known standbyHosts, if we haven't already selected it for failover.
     * 3. If still dont find a host, we fall back to AnyHost.
     *
     * See https://issues.apache.org/jira/browse/SAMZA-2140
     *
     * @param activeContainerID Samza containerID of the active container
     * @param activeContainerResourceID ResourceID of the active container at the time of its last failure
     * @return standby host for the active container (if found), Any-host otherwise.
     */
    private String selectStandbyHost(String activeContainerID, String activeContainerResourceID) {

      LOG.info("Standby containers {} for active container {} (resourceID {})", this.standbyContainerConstraints.get(activeContainerID), activeContainerID, activeContainerResourceID);

      // obtain any existing failover metadata
      Optional<FailoverMetadata> failoverMetadata = getFailoverMetadata(activeContainerResourceID);

      // Iterate over the list of running standby containers, to find a standby resource that we have not already
      // used for a failover for this active resoruce
      for (String standbyContainerID : this.standbyContainerConstraints.get(activeContainerID)) {

        if (samzaApplicationState.runningProcessors.containsKey(standbyContainerID)) {
          SamzaResource standbyContainerResource = samzaApplicationState.runningProcessors.get(standbyContainerID);

          // use this standby if there was no previous failover for which this standbyResource was used
          if (!(failoverMetadata.isPresent() && failoverMetadata.get()
              .isStandbyResourceUsed(standbyContainerResource.getContainerId()))) {

            LOG.info("Returning standby container {} in running state on host {} for active container {}",
                standbyContainerID, standbyContainerResource.getHost(), activeContainerID);
            return standbyContainerResource.getHost();
          }
        }
      }
      LOG.info("Did not find any running standby container for active container {}", activeContainerID);

      // We iterate over the list of last-known standbyHosts to check if anyone of them has not already been tried
      for (String standbyContainerID : this.standbyContainerConstraints.get(activeContainerID)) {

        String standbyHost = samzaApplicationState.jobModelManager.jobModel().
            getContainerToHostValue(standbyContainerID, SetContainerHostMapping.HOST_KEY);

        if (standbyHost == null || standbyHost.isEmpty()) {
          LOG.info("No last known standbyHost for container {}", standbyContainerID);

        } else if (failoverMetadata.isPresent() && failoverMetadata.get().isStandbyHostUsed(standbyHost)) {

          LOG.info("Not using standby host {} for active container {} because it had already been selected", standbyHost,
              activeContainerID);
        } else {
          // standbyHost is valid and has not already been selected
          LOG.info("Returning standby host {} for active container {}", standbyHost, activeContainerID);
          return standbyHost;
        }
      }

      LOG.info("Did not find any standby host for active container {}, returning any-host", activeContainerID);
      return ResourceRequestState.ANY_HOST;
    }

    /**
     * Register the failure of an active container (identified by its resource ID).
     */
    private FailoverMetadata registerActiveContainerFailure(String activeContainerID, String activeContainerResourceID) {

      // this active container's resource ID is already registered, in which case update the metadata
      FailoverMetadata failoverMetadata;
      if (failovers.containsKey(activeContainerResourceID)) {
        failoverMetadata = failovers.get(activeContainerResourceID);
      } else {
        failoverMetadata = new FailoverMetadata(activeContainerID, activeContainerResourceID);
      }
      this.failovers.put(activeContainerResourceID, failoverMetadata);
      return failoverMetadata;
    }

    /**
     * Check if this standbyContainerResource is present in the failoverState for an active container.
     * This is used to determine if we requested a stop a container.
     */
    private Optional<FailoverMetadata> checkIfUsedForFailover(String standbyContainerResourceId) {

      if (standbyContainerResourceId == null) {
        return Optional.empty();
      }

      for (FailoverMetadata failoverMetadata : failovers.values()) {
        if (failoverMetadata.isStandbyResourceUsed(standbyContainerResourceId)) {
          LOG.info("Standby container with resource id {} was selected for failover of active container {}",
              standbyContainerResourceId, failoverMetadata.activeContainerID);
          return Optional.of(failoverMetadata);
        }
      }
      return Optional.empty();
    }

    /**
     * Check if matching this SamzaResourceRequest to the given resource, meets all standby-container container constraints.
     *
     * @param request The resource request to match.
     * @param samzaResource The samzaResource to potentially match the resource to.
     * @return
     */
    private boolean checkStandbyConstraints(SamzaResourceRequest request, SamzaResource samzaResource) {
      String containerIDToStart = request.getProcessorId();
      String host = samzaResource.getHost();
      List<String> containerIDsForStandbyConstraints = this.standbyContainerConstraints.get(containerIDToStart);

      // Check if any of these conflicting containers are running/launching on host
      for (String containerID : containerIDsForStandbyConstraints) {
        SamzaResource resource = samzaApplicationState.pendingProcessors.get(containerID);

        // return false if a conflicting container is pending for launch on the host
        if (resource != null && resource.getHost().equals(host)) {
          LOG.info("Container {} cannot be started on host {} because container {} is already scheduled on this host",
              containerIDToStart, samzaResource.getHost(), containerID);
          return false;
        }

        // return false if a conflicting container is running on the host
        resource = samzaApplicationState.runningProcessors.get(containerID);
        if (resource != null && resource.getHost().equals(host)) {
          LOG.info("Container {} cannot be started on host {} because container {} is already running on this host",
              containerIDToStart, samzaResource.getHost(), containerID);
          return false;
        }
      }

      return true;
    }

    /**
     *  Attempt to the run a container on the given candidate resource, if doing so meets the standby container constraints.
     * @param request The Samza container request
     * @param preferredHost the preferred host associated with the container
     * @param samzaResource the resource candidate
     */
    public void checkStandbyConstraintsAndRunStreamProcessor(SamzaResourceRequest request, String preferredHost,
        SamzaResource samzaResource, AbstractContainerAllocator containerAllocator,
        ResourceRequestState resourceRequestState) {
      String containerID = request.getProcessorId();

      if (checkStandbyConstraints(request, samzaResource)) {
        // This resource can be used to launch this container
        LOG.info("Running container {} on {} meets standby constraints, preferredHost = {}", containerID,
            samzaResource.getHost(), preferredHost);
        containerAllocator.runStreamProcessor(request, preferredHost);
      } else if (StandbyTaskUtil.isStandbyContainer(containerID)) {
        // This resource cannot be used to launch this standby container, so we make a new anyhost request
        LOG.info(
            "Running standby container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource, and making a new ANY_HOST request",
            containerID, samzaResource.getHost());
        resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
        resourceRequestState.cancelResourceRequest(request);
        containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
        samzaApplicationState.failedStandbyAllocations.incrementAndGet();
      } else {
        // This resource cannot be used to launch this active container container, so we initiate a failover
        LOG.warn(
            "Running active container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource",
            containerID, samzaResource.getHost());
        resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
        resourceRequestState.cancelResourceRequest(request);

        Optional<FailoverMetadata> failoverMetadata = getFailoverMetadata(request);
        String lastKnownResourceID =
            failoverMetadata.isPresent() ? failoverMetadata.get().activeContainerResourceID : "unknown-" + containerID;
        initiateStandbyAwareAllocation(containerID, lastKnownResourceID, containerAllocator);
        samzaApplicationState.failedStandbyAllocations.incrementAndGet();
      }
    }

    /**
     * Handle an expired resource request
     * @param containerID the containerID for which the resource request was made
     * @param request the expired resource request
     * @param alternativeResource an alternative, already-allocated, resource (if available)
     * @param containerAllocator the container allocator (used to issue any required subsequent resource requests)
     * @param resourceRequestState used to cancel resource requests if required.
     */
    public void handleExpiredResourceRequest(String containerID, SamzaResourceRequest request,
        Optional<SamzaResource> alternativeResource, AbstractContainerAllocator containerAllocator,
        ResourceRequestState resourceRequestState) {

      if (StandbyTaskUtil.isStandbyContainer(containerID)) {
        handleExpiredRequestForStandbyContainer(containerID, request, alternativeResource, containerAllocator,
            resourceRequestState);
      } else {
        handleExpiredRequestForActiveContainer(containerID, request, containerAllocator, resourceRequestState);
      }
    }

    // Handle an expired resource request that was made for placing a standby container
    private void handleExpiredRequestForStandbyContainer(String containerID, SamzaResourceRequest request,
        Optional<SamzaResource> alternativeResource, AbstractContainerAllocator containerAllocator,
        ResourceRequestState resourceRequestState) {

      if (alternativeResource.isPresent()) {
        // A standby container can be started on the anyhost-alternative-resource rightaway provided it passes all the
        // standby constraints
        LOG.info("Handling expired request, standby container {} can be started on alternative resource {}", containerID,
            alternativeResource.get());
        checkStandbyConstraintsAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST, alternativeResource.get(),
            containerAllocator, resourceRequestState);
      } else {
        // If there is no alternative-resource for the standby container we make a new anyhost request
        LOG.info("Handling expired request, requesting anyHost resource for standby container {}", containerID);
        resourceRequestState.cancelResourceRequest(request);
        containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
      }
    }

    // Handle an expired resource request that was made for placing an active container
    private void handleExpiredRequestForActiveContainer(String containerID, SamzaResourceRequest request,
        AbstractContainerAllocator containerAllocator, ResourceRequestState resourceRequestState) {

      LOG.info("Handling expired request for active container {}", containerID);
      Optional<FailoverMetadata> failoverMetadata = getFailoverMetadata(request);
      resourceRequestState.cancelResourceRequest(request);

      // we use the activeContainer's resourceID to initiate the failover and index failover-state
      // if there is no prior failure for this active-Container, we use "unknown"
      String lastKnownResourceID =
          failoverMetadata.isPresent() ? failoverMetadata.get().activeContainerResourceID : "unknown-" + containerID;
      LOG.info("Handling expired request for active container {}, lastKnownResourceID is {}", containerID, lastKnownResourceID);
      initiateStandbyAwareAllocation(containerID, lastKnownResourceID, containerAllocator);
    }

    /**
     * Check if a activeContainerResource has failover-metadata associated with it
     */
    private Optional<FailoverMetadata> getFailoverMetadata(String activeContainerResourceID) {
      return this.failovers.containsKey(activeContainerResourceID) ? Optional.of(
          this.failovers.get(activeContainerResourceID)) : Optional.empty();
    }

    /**
     * Check if a SamzaResourceRequest was issued for a failover.
     */
    private Optional<FailoverMetadata> getFailoverMetadata(SamzaResourceRequest resourceRequest) {
      for (FailoverMetadata failoverMetadata : this.failovers.values()) {
        if (failoverMetadata.containsResourceRequest(resourceRequest)) {
          return Optional.of(failoverMetadata);
        }
      }
      return Optional.empty();
    }

    @Override
    public String toString() {
      return this.failovers.toString();
    }


  }

}