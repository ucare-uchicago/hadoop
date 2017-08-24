package org.apache.hadoop.yarn.samc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * 
 * @author riza
 *
 */
public class StatusNotifier extends Thread {

  private static final Log LOG = LogFactory.getLog(StatusNotifier.class);

  Configuration conf;

  int sendNode;
  int nodeState;
  NodeState enumNodeState;

  String fileDir;
  String filename;
  File eventFile;

  boolean samcResponse;

  public StatusNotifier(NodeRole roleS, NodeState nodeState) {
    conf = new YarnConfiguration();

    this.sendNode = roleS.ordinal();
    this.nodeState = nodeState.ordinal();
    this.enumNodeState = nodeState;
    this.fileDir = conf.get(YarnConfiguration.SAMC_IPC_DIR);
    this.filename = createFileName();
    this.samcResponse = false;

    this.eventFile = new File(fileDir + "/new/" + filename);
  }

  public void submit() {
    // begin to write /new/filename
    writeNewEvent();
    commitEvent();
  }

  private void writeNewEvent() {
    try {
      PrintWriter writer = new PrintWriter(eventFile, "UTF-8");
      writer.print("sendNode=" + sendNode + "\n");
      writer.print("nodeState=" + nodeState + "\n");
      writer.close();
    } catch (IOException e) {
      LOG.error("samc: Error on writing new event file", e);
    }
  }

  private void commitEvent() {
    try {
      File commitFile = new File(fileDir + "/send/" + filename);
      eventFile.renameTo(commitFile);
      LOG.debug("samc: Event committed to " + eventFile.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String getFileDir() {
    return fileDir;
  }

  public String createFileName() {
    long time = System.nanoTime();
    long postfix = time % 1000000;
    String filename =
        "hadoopUpdate-" + String.valueOf(nodeState) + "-" + String.valueOf(postfix);
    return filename;
  }

  public int getEventId() {
    int prime = 19;
    int hash = 1;
    hash = prime * hash + sendNode;
    hash = prime * hash + nodeState;
    return hash;
  }

  public boolean hasSAMCResponse() {
    return samcResponse;
  }

  @Override
  public String toString() {
    return filename + "={sendNode: " + sendNode + ", nodeState: "
        + enumNodeState + "}";
  }

  public void printToLog() {
    LOG.info("samc: " + this.toString());
  }
}
