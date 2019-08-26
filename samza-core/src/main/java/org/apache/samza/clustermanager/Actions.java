package org.apache.samza.clustermanager;

public interface Actions {
  void handleContainerStop();
  void handleContainerLaunchFail();
  void handleContainerLaunchSuccess();
  void handleContainerExpiredRequests();
}
