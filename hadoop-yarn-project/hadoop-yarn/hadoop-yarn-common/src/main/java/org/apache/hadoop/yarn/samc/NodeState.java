package org.apache.hadoop.yarn.samc;

public enum NodeState {
  DEAD,
  ALIVE,
//  RM_AM_FINISHED,
  RM_AM_RECOVERING,
  AM_INIT,
  AM_RUNNING,
  AM_UNREGISTERED
}
