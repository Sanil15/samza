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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerPlacementManager {

  private static final Logger log = LoggerFactory.getLogger(ContainerPlacementManager.class);

  private final SamzaApplicationState samzaApplicationState;

  // Resource-manager, used to stop containers
  private ClusterResourceManager clusterResourceManager;

  private final Map<String, FailoverMetadata> failovers;


  public boolean checkHostRequestedForMove(String hostname) {
    for(FailoverMetadata metadata: failovers.values()) {
      if (metadata.resourceRequest.getPreferredHost().equals(hostname))
        return true;
    }
    return false;
  }


  public ContainerPlacementManager(SamzaApplicationState samzaApplicationState,
      ClusterResourceManager clusterResourceManager) {
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    this.failovers = new HashMap<>();
  }

  public synchronized boolean initiaiteFailover(String activeContainerProcessorID, String preferredHost) {
    if (!getMoveMetadata(activeContainerProcessorID).get().containerShutdownRequested) {
      log.info("Move Request: Found an available container for Processor ID: {} on the preferred host: {}", activeContainerProcessorID, preferredHost);
      log.info("Active Container Shutdown requested and failover is initiated");
      this.clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(activeContainerProcessorID));
      getMoveMetadata(activeContainerProcessorID).get().setContainerShutdownRequested();
      return true;
    }
    return false;
    // log.info("Active Container Shutdown already Requested waiting for Container to Stop");
  }

  /**
   * Register the failure of an active container (identified by its resource ID).
   */
  public synchronized FailoverMetadata registerContainerMove(String activeContainerID, String activeContainerSamzaProcessorID, String currentHost, SamzaResourceRequest resourceRequest) {
    // this active container's resource ID is already registered, in which case update the metadata
    FailoverMetadata failoverMetadata;
    if (failovers.containsKey(activeContainerSamzaProcessorID)) {
      log.info("Already a failover is requested: {}, can't accept another one", failovers.get(activeContainerSamzaProcessorID));
      failoverMetadata = null;
    } else {
      failoverMetadata = new FailoverMetadata(activeContainerID, activeContainerSamzaProcessorID, currentHost, resourceRequest);
      log.info("ContainerMoveAction Registering a new request failover with metadata {}", failoverMetadata);
      this.failovers.put(activeContainerSamzaProcessorID, failoverMetadata);
    }
    return failoverMetadata;
  }

  public synchronized void markMoveComplete(String activeContainerResourceID) {
    log.info("Marking the move complete for Samza Container with Processor ID", activeContainerResourceID);
    this.failovers.remove(activeContainerResourceID);
  }

  public synchronized void markMoveFailed(String activeContainerResourceID) {
    log.info("Marking the move failed for Samza Container with Processor ID", activeContainerResourceID);
    this.failovers.remove(activeContainerResourceID);
  }


  /**
   * Check if a activeContainerResource has failover-metadata associated with it
   */
  public Optional<FailoverMetadata> getMoveMetadata(String activeContainerSamzaProcessorId) {
    return this.failovers.containsKey(activeContainerSamzaProcessorId) ?
        Optional.of(this.failovers.get(activeContainerSamzaProcessorId)) : Optional.empty();
  }

//
//  /**
//   * Check if a SamzaResourceRequest was issued for a failover.
//   */
//  private FailoverMetadata getFailoverMetadata(SamzaResourceRequest resourceRequest) {
//    for (FailoverMetadata failoverMetadata : this.failovers.values()) {
//      if (failoverMetadata.containsResourceRequest(resourceRequest)) {
//        return failoverMetadata;
//      }
//    }
//    return null;
//  }


  /**
   * Encapsulates metadata concerning the failover of an active container.
   */
  public class FailoverMetadata {
    public final String activeContainerID;
    public final String activeContainerSamzaProcessorId;
    public final String currentHost;
    // Resource requests issued during this failover
    private final SamzaResourceRequest resourceRequest;

    private Integer countOfMoves;
    private long lastAllocatorRequestTime;
    private boolean containerShutdownRequested;

    public FailoverMetadata(String activeContainerID, String activeContainerSamzaProcessorId, String currentHost, SamzaResourceRequest resourceRequest) {
      this.activeContainerID = activeContainerID;
      this.activeContainerSamzaProcessorId = activeContainerSamzaProcessorId;
      this.currentHost = currentHost;
      this.resourceRequest = resourceRequest;
      this.countOfMoves = 0;
      this.containerShutdownRequested = false;
      this.lastAllocatorRequestTime = System.currentTimeMillis();
    }

    public long getLastAllocatorRequestTime() {
      return lastAllocatorRequestTime;
    }

    public void setAllocatorRequestTime() {
      this.lastAllocatorRequestTime = System.currentTimeMillis();
    }

    public Integer getCountOfMoves() {
      return countOfMoves;
    }

    public boolean isContainerShutdownRequested() {
      return containerShutdownRequested;
    }

    public void setContainerShutdownRequested() {
      this.containerShutdownRequested = true;
    }

    public void incrementContainerMoveRequestCount() {
      this.countOfMoves++;
    }

    public void setCountOfMoves(Integer countOfMoves) {
      this.countOfMoves = countOfMoves;
    }

    public String getActiveContainerID() {
      return activeContainerID;
    }

    public String getActiveContainerSamzaProcessorId() {
      return activeContainerSamzaProcessorId;
    }

    public String getCurrentHost() {
      return currentHost;
    }

    public SamzaResourceRequest getResourceRequest() {
      return resourceRequest;
    }

    @Override
    public String toString() {
      return "[activeContainerID: " + this.activeContainerID + " activeContainerSamzaProcessorId: "
          + this.activeContainerSamzaProcessorId + " resourceRequests: " + resourceRequest + "]";
    }
  }


  //
//    // Add the samzaResourceRequest to the list of resource requests associated with this failover
//    public synchronized void recordResourceRequest(SamzaResourceRequest samzaResourceRequest) {
//      this.resourceRequests.offer(samzaResourceRequest);
//    }
//
//    // Check if this resource request has been issued in this failover
//    public synchronized boolean containsResourceRequest(SamzaResourceRequest samzaResourceRequest) {
//      return this.resourceRequests.contains(samzaResourceRequest);
//    }
//
//    public SamzaResourceRequest getResourceRequests() {
//      return resourceRequests.peek();
//    }
//
//    public synchronized void markRequestComplete(SamzaResourceRequest samzaResourceRequest) {
//      if(resourceRequests.peek().compareTo(samzaResourceRequest) != 0) {
//        throw new SamzaException("Request at the head of the queue is not one that is served / completed");
//      }
//      resourceRequests.poll();
//    }



}
