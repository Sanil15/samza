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

import java.util.HashSet;
import java.util.Set;


public class ControlActionMetaData {

  // Logical container id 0,1,2,3,
  private final String activeContainerSamzaProcessorId;
  // Last known deployment id of the container
  private final String activeContainerID;
  private final String sourceHost;
  // Resource requests issued during this failover
  private final Set<SamzaResourceRequest> resourceRequests;

  private Integer countOfMoves;
  private long lastAllocatorRequestTime;
  private boolean containerShutdownRequested;

  public ControlActionMetaData(String activeContainerSamzaProcessorId, String activeContainerID, String sourceHost) {
    this.activeContainerID = activeContainerID;
    this.activeContainerSamzaProcessorId = activeContainerSamzaProcessorId;
    this.sourceHost = sourceHost;
    this.resourceRequests = new HashSet<>();
    this.countOfMoves = 0;
    this.containerShutdownRequested = false;
    this.lastAllocatorRequestTime = System.currentTimeMillis();
  }

  public synchronized boolean isContainerShutdownRequested() {
    return containerShutdownRequested;
  }

  public synchronized void setContainerShutdownRequested() {
    this.containerShutdownRequested = true;
  }

  // Add the samzaResourceRequest to the list of resource requests associated with this failover
  public synchronized void recordResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    this.resourceRequests.add(samzaResourceRequest);
  }

  // Check if this resource request has been issued in this failover
  public synchronized boolean containsResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    return this.resourceRequests.contains(samzaResourceRequest);
  }
  @Override
  public String toString() {
    return "[activeContainerID: " + this.activeContainerID + " activeContainerSamzaProcessorId: "
        + this.activeContainerSamzaProcessorId + " resourceRequests: " + resourceRequests + "]";
  }

}

//
//  public long getLastAllocatorRequestTime() {
//    return lastAllocatorRequestTime;
//  }
//
//  public void setAllocatorRequestTime() {
//    this.lastAllocatorRequestTime = System.currentTimeMillis();
//  }
//
//  public Integer getCountOfMoves() {
//    return countOfMoves;
//  }
//
//  public boolean isContainerShutdownRequested() {
//    return containerShutdownRequested;
//  }
//
//  public void setContainerShutdownRequested() {
//    this.containerShutdownRequested = true;
//  }
//
//  public void incrementContainerMoveRequestCount() {
//    this.countOfMoves++;
//  }
//
//  public void setCountOfMoves(Integer countOfMoves) {
//    this.countOfMoves = countOfMoves;
//  }
//
//  public String getActiveContainerID() {
//    return activeContainerID;
//  }
//
//  public String getActiveContainerSamzaProcessorId() {
//    return activeContainerSamzaProcessorId;
//  }
//
//  public String getCurrentHost() {
//    return currentHost;
//  }
