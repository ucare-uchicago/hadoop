package org.apache.hadoop.mapreduce.v2.app;

/**
 * Created by huanke on 11/27/16.
 */
public enum InterceptEventType {
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
