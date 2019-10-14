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
  private final String processorId;
  // Last known deployment id of the container
  private final String containerId;
  private final String sourceHost;
  private final String destinationHost;
  // Expiry timeout for current request
  private final Long requestExiryTimeout;
  // Resource requests issued during this failover
  private final Set<SamzaResourceRequest> resourceRequests;

  private Integer countOfMoves;
  private long lastAllocatorRequestTime;
  private boolean containerShutdownRequested;
  private boolean activeContainerStopped;


  public ControlActionMetaData(String processorId, String containerId, String sourceHost, String destinationHost, Long requestExiryTimeout) {
    this.containerId = containerId;
    this.processorId = processorId;
    this.sourceHost = sourceHost;
    this.destinationHost = destinationHost;
    this.resourceRequests = new HashSet<>();
    this.countOfMoves = 0;
    this.containerShutdownRequested = false;
    this.lastAllocatorRequestTime = System.currentTimeMillis();
    this.activeContainerStopped = false;
    this.requestExiryTimeout = requestExiryTimeout;
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

  public synchronized void setActiveContainerStopped(){
    activeContainerStopped = true;
  }

  public synchronized boolean getActiveConatainerStopped(){
    return activeContainerStopped;
  }

  public String getSourceHost() {
    return sourceHost;
  }

  public Long getRequestActionExpiryTimeout() {
    return requestExiryTimeout;
  }

  @Override
  public String toString() {
    return "ControlActionMetaData{" + "processorId='" + processorId + '\'' + ", containerId='" + containerId + '\''
        + ", sourceHost='" + sourceHost + '\'' + ", destinationHost='" + destinationHost + '\'' + ", resourceRequests="
        + resourceRequests + ", countOfMoves=" + countOfMoves + ", lastAllocatorRequestTime=" + lastAllocatorRequestTime
        + ", containerShutdownRequested=" + containerShutdownRequested + ", activeContainerStopped="
        + activeContainerStopped + '}';
  }

  public String getDestinationHost() {
    return destinationHost;
  }

}