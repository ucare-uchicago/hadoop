package org.apache.hadoop.yarn.samc;

public enum NodeState {
  DEAD,
  ALIVE,
  AM_INIT,
  AM_RUNNING,
  AM_COMMITTING
}
