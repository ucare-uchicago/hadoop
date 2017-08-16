package org.apache.hadoop.yarn.samc;

/**
 * 
 * @author riza
 *
 */
public enum InterceptedEventType {
  //AMLauncher startContainer()  (RM--> NM)
  AMLauncheee,
  AMCleanup,
  Remove_StateStore, //RM --> RM
  Store_StateStore,
  
  //RMCommunicator (AM--> RM)
  AMRegister,
  AMUnRegister,
  //RMContainerRequestor
  AllocateContainer,
  HeartbeatWith, //If there are resource request from AM --> RM, Otherwise there are some HB (empty)

  //ContainerLauncherImpl (AM --> NM)
  CONTAINER_REMOTE_LAUNCH,
  CONTAINER_REMOTE_CLEANUP
}
