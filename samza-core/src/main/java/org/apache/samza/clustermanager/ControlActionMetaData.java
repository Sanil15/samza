package org.apache.samza.clustermanager;

public class ControlActionMetaData {

  private final String activeContainerID;
  private final String activeContainerSamzaProcessorId;
  private final String currentHost;
  // Resource requests issued during this failover
  private final SamzaResourceRequest resourceRequest;

  private Integer countOfMoves;
  private long lastAllocatorRequestTime;
  private boolean containerShutdownRequested;

  public ControlActionMetaData(String activeContainerID, String activeContainerSamzaProcessorId, String currentHost, SamzaResourceRequest resourceRequest) {
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
