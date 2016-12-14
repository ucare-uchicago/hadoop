package org.apache.hadoop.yarn.server.resourcemanager;

/**
 * Created by huanke on 11/27/16.
 */
public enum RMInterceptEventType {
    //AMLauncher startContainer()  (RM--> NM)
    AMLauncheee,
    AMCleanup,
    Remove_StateStore, //RM --> RM
    Store_StateStore
}
