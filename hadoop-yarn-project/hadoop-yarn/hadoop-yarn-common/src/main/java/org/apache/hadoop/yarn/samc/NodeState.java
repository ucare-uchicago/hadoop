package org.apache.hadoop.yarn.samc;

public enum NodeState {
  DEAD,
  ALIVE,
  RM_RECOVERING,
  AM_INIT,
  AM_RUNNING,
  AM_COMMITTING
}
