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

import java.time.Duration;
import java.util.Map;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the default allocator that will be used by ContainerProcessManager.
 *
 * When host-affinity is not enabled, this periodically wakes up to assign a processor to *ANY* allocated resource.
 * If there aren't enough resources, it waits by sleeping for {@code allocatorSleepIntervalMs} milliseconds.
 *
 * This class is instantiated by the ContainerProcessManager (which in turn is created by the JC from run-jc.sh),
 * when host-affinity is off. Otherwise, the HostAwareContainerAllocator is instantiated.
 */
public class ContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(ContainerAllocator.class);

  public ContainerAllocator(ClusterResourceManager manager,
                            Config config, SamzaApplicationState state, ContainerPlacementManager containerPlacementManager) {
    super(manager, new ResourceRequestState(false, manager), config, state, containerPlacementManager);
  }

  /**
   * During the run() method, the thread sleeps for allocatorSleepIntervalMs ms. It then invokes assignResourceRequests,
   * and tries to allocate any unsatisfied request that is still in the request queue {@link ResourceRequestState})
   * with allocated resources, if any.
   *
   * Since host-affinity is not enabled, all allocated resources are buffered in the list keyed by "ANY_HOST".
   * */
  @Override
  public void assignResourceRequests() {
    while (hasPendingRequest()) {
      SamzaResourceRequest request = peekPendingRequest();
      String processorId = request.getProcessorId();
      String preferredHost = request.getPreferredHost();
      long requestCreationTime = request.getRequestTimestampMs(); // todo: timeout for expired resources, cancel and release

      if (containerPlacementManager.getMoveMetadata(processorId).isPresent()) {
        // TO do check has request expired then return
        // log.info("Found a move request for processor id {} on a preferred host {}", processorId, preferredHost);

        // If failover is already under going do not do anything
        if (containerPlacementManager.getMoveMetadata(processorId).get().isContainerShutdownRequested()) {
          break;
        }

        if(hasAllocatedResource(preferredHost)) {
         containerPlacementManager.initiaiteFailover(processorId, preferredHost);
        } else {
          // Maintain state on how many failed move requests from yarn happened, if that surpasses a configured max
          // or has timed out then remove the move requests for that contain

          // mark move failed if not able to issue resource requests
          // Release & Retry: Release Unstartable Container and make a new move request to container allocator

          long lastAllocatorRequest = System.currentTimeMillis() - containerPlacementManager.getMoveMetadata(processorId).get().getLastAllocatorRequestTime();

          // Request Every 10 seconds for 3 times
          if (lastAllocatorRequest > Duration.ofSeconds(10).toMillis() && containerPlacementManager.getMoveMetadata(processorId).get().getCountOfMoves() <= 3) {
            // wait for one allocated resource
            log.info("Move constraints are not satisfied requesting resources since ");
            log.info("Requesting a new resource again");

            // Cancel the existing request
            resourceRequestState.cancelResourceRequest(request);
            resourceRequestState.releaseExtraResources();

            // Issue a new request
            requestResource(processorId, preferredHost);
            containerPlacementManager.getMoveMetadata(processorId).get().incrementContainerMoveRequestCount();
            containerPlacementManager.getMoveMetadata(processorId).get().setAllocatorRequestTime();
          }

          // Try X number of times, potential problem this needs to happen with a timeout, try 3 times but wait for x
          // seconds to ensure Yarn allocates resources for you! or when request is issued 3 times by container allocator
          boolean requestExpired =  System.currentTimeMillis() - request.getRequestTimestampMs() > Duration.ofMinutes(2).toMillis();

          if (requestExpired) {
            log.info("Your Move Container Request expired doing nothing");
            resourceRequestState.cancelResourceRequest(request);
            // extra resources are already released by the Allocator Thread so you do not need to worry :)
            containerPlacementManager.markMoveFailed(processorId);
            break;
          }
          // break otherwise it will keep looping since you made another async call for requested Container
          break;
        }
      }
      else if(hasAllocatedResource(ResourceRequestState.ANY_HOST)) {
        log.info("Invoking a non move request {} to ANY_HOST", request.toString());
        runStreamProcessor(request, ResourceRequestState.ANY_HOST);
      } else {
        break;
      }
    }
  }

  /**
   * Since host-affinity is not enabled, the processor id to host mappings will be ignored and all resources will be
   * matched to any available host.
   *
   * @param processorToHostMapping A Map of [processorId, hostName] ID of the processor to run on the resource.
   *                               The hostName will be ignored and each processor will be matched to any available host.
   */
  @Override
  public void requestResources(Map<String, String> processorToHostMapping)  {
    for (Map.Entry<String, String> entry : processorToHostMapping.entrySet()) {
      String processorId = entry.getKey();
      requestResource(processorId, ResourceRequestState.ANY_HOST);
    }
  }
}
