package org.apache.hadoop.yarn.samc;

/**
 * 
 * @author riza
 *
 */
public enum InterceptedEventType {
  //AMLauncher startContainer()  (RM--> NM)
  RM_NM_AMLAUNCH,
  RM_NM_AMCLEANUP,
  RM_STATESTORE_REMOVE, //RM --> RM
  RM_STATESTORE_ADD,

  //RMCommunicator (AM--> RM)
  AM_RM_REGISTER,
  AM_RM_UNREGISTER,
  //RMContainerRequestor
  AM_RM_ALLOCATE_CONTAINER,
  AM_RM_HEARTBEAT, //If there are resource request from AM --> RM, Otherwise there are some HB (empty)

  // ApplicationImpl (NM --> AM)
  NM_AM_INIT_DONE,

  //ContainerLauncherImpl (AM --> NM)
  AM_NM_CONTAINER_REMOTE_LAUNCH,
  AM_NM_CONTAINER_REMOTE_CLEANUP
}
