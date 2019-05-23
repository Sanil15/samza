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

  private static final Logger log = LoggerFactory.getLogger(StandbyContainerManager.class);

  private final SamzaApplicationState samzaApplicationState;

  // Resource-manager, used to stop containers
  private ClusterResourceManager clusterResourceManager;

  private final Map<String, FailoverMetadata> failovers;


  public ContainerPlacementManager(SamzaApplicationState samzaApplicationState,
      ClusterResourceManager clusterResourceManager) {
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    this.failovers = new HashMap<>();
  }

  public synchronized void initiaiteFailover(String activeContainerID, String preferredHost) {
    this.clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(activeContainerID));
    // update samza application state
  }

  /**
   * Register the failure of an active container (identified by its resource ID).
   */
  public synchronized FailoverMetadata registerContainerMove(String activeContainerID, String activeContainerResourceID, String currentHost, SamzaResourceRequest resourceRequest) {
    // this active container's resource ID is already registered, in which case update the metadata
    FailoverMetadata failoverMetadata;
    if (failovers.containsKey(activeContainerResourceID)) {
      log.info("Already a failover is requested: {}, can't accept another one", failovers.get(activeContainerResourceID));
      failoverMetadata = null;
    } else {
      failoverMetadata = new FailoverMetadata(activeContainerID, activeContainerResourceID, currentHost, resourceRequest);
      log.info("Registering a new failover with metadata {}", failoverMetadata);
      this.failovers.put(activeContainerResourceID, failoverMetadata);
    }
    return failoverMetadata;
  }

  public synchronized void markMoveComplete(String activeContainerResourceID) {
    this.failovers.remove(activeContainerResourceID);
  }

  /**
   * Check if a activeContainerResource has failover-metadata associated with it
   */
  public Optional<FailoverMetadata> getMoveMetadata(String activeContainerResourceID) {
    return this.failovers.containsKey(activeContainerResourceID) ?
        Optional.of(this.failovers.get(activeContainerResourceID)) : Optional.empty();
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
    public final String activeContainerResourceID;
    public final String currentHost;
    // Resource requests issued during this failover
    private final SamzaResourceRequest resourceRequest;

    public FailoverMetadata(String activeContainerID, String activeContainerResourceID, String currentHost, SamzaResourceRequest resourceRequest) {
      this.activeContainerID = activeContainerID;
      this.activeContainerResourceID = activeContainerResourceID;
      this.currentHost = currentHost;
      this.resourceRequest = resourceRequest;
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

    public String getActiveContainerID() {
      return activeContainerID;
    }

    public String getActiveContainerResourceID() {
      return activeContainerResourceID;
    }

    public String getCurrentHost() {
      return currentHost;
    }

    public SamzaResourceRequest getResourceRequest() {
      return resourceRequest;
    }

    @Override
    public String toString() {
      return "[activeContainerID: " + this.activeContainerID + " activeContainerResourceID: "
          + this.activeContainerResourceID + " resourceRequests: " + resourceRequest + "]";
    }
  }


}
