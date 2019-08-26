package org.apache.samza.clustermanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class FailoverMetadata {
  public final String activeContainerID;
  public final String activeContainerResourceID;

  // Map of samza-container-resource ID to host, for each standby container selected for failover of the activeContainer
  private final Map<String, String> selectedStandbyContainers;

  // Resource requests issued during this failover
  private final Set<SamzaResourceRequest> resourceRequests;

  public FailoverMetadata(String activeContainerID, String activeContainerResourceID) {
    this.activeContainerID = activeContainerID;
    this.activeContainerResourceID = activeContainerResourceID;
    this.selectedStandbyContainers = new HashMap<>();
    resourceRequests = new HashSet<>();
  }

  // Check if this standbyContainerResourceID was used in this failover
  public synchronized boolean isStandbyResourceUsed(String standbyContainerResourceID) {
    return this.selectedStandbyContainers.keySet().contains(standbyContainerResourceID);
  }

  // Get the hostname corresponding to the standby resourceID
  public synchronized String getStandbyContainerHostname(String standbyContainerResourceID) {
    return selectedStandbyContainers.get(standbyContainerResourceID);
  }

  // Add the standbyContainer resource to the list of standbyContainers used in this failover
  public synchronized void updateStandbyContainer(String standbyContainerResourceID, String standbyContainerHost) {
    this.selectedStandbyContainers.put(standbyContainerResourceID, standbyContainerHost);
  }

  // Add the samzaResourceRequest to the list of resource requests associated with this failover
  public synchronized void recordResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    this.resourceRequests.add(samzaResourceRequest);
  }

  // Check if this resource request has been issued in this failover
  public synchronized boolean containsResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    return this.resourceRequests.contains(samzaResourceRequest);
  }

  // Check if this standby-host has been used in this failover, that is, if this host matches the host of a selected-standby,
  // or if we have made a resourceRequest for this host
  public boolean isStandbyHostUsed(String standbyHost) {
    return this.selectedStandbyContainers.values().contains(standbyHost)
        || this.resourceRequests.stream().filter(request -> request.getPreferredHost().equals(standbyHost)).count() > 0;
  }

  @Override
  public String toString() {
    return "[activeContainerID: " + this.activeContainerID + " activeContainerResourceID: "
        + this.activeContainerResourceID + " selectedStandbyContainers:" + selectedStandbyContainers
        + " resourceRequests: " + resourceRequests + "]";
  }
}
