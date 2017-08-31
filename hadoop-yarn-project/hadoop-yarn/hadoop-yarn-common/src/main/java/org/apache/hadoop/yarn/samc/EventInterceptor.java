package org.apache.hadoop.yarn.samc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
public class EventInterceptor {

  private static final Log LOG = LogFactory.getLog(EventInterceptor.class);

  Configuration conf;

  int sendNode;
  int recvNode;
  int nodeState; // 1 represents alived and 0 represents crashed.
  InterceptedEventType eventType;
  int eventTypeId;
  int eventId;

  String fileDir;
  String filename;
  File eventFile;

  boolean samcResponse;

  public EventInterceptor(NodeRole roleS, NodeRole roleR, NodeState nodeState,
      InterceptedEventType interceptEventType) {
    conf = new YarnConfiguration();

    this.sendNode = roleS.ordinal();
    this.recvNode = roleR.ordinal();
    this.nodeState = nodeState.ordinal();
    this.eventType = interceptEventType;
    this.eventTypeId = interceptEventType.ordinal();
    this.eventId = getEventId();
    this.fileDir = conf.get(YarnConfiguration.SAMC_IPC_DIR);
    this.filename = createFileName();
    this.samcResponse = false;

    this.eventFile = new File(fileDir + "/new/" + filename);
  }

  public InterceptedEventType getEventType() {
    return eventType;
  }

  public int getEventTypeId() {
    return eventTypeId;
  }

  public void submitAndWait() {
    // begin to write /new/filename
    writeNewEvent();
    commitEvent();
    waitAck();
  }

  private void writeNewEvent() {
    try {
      PrintWriter writer = new PrintWriter(eventFile, "UTF-8");
      writer.print("sendNode=" + sendNode + "\n");
      writer.print("recvNode=" + recvNode + "\n");
      writer.print("nodeState=" + nodeState + "\n");
      writer.print("eventTypeId=" + eventTypeId + "\n");
      writer.print("eventType=" + eventType + "\n");
      writer.print("eventId=" + eventId);
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

  private void waitAck() {
    String ackFileName = fileDir + "/ack/" + filename;
    FileReader fileReader = null;
    BufferedReader inRead = null;
    File ackFile = new File(ackFileName);

    try {

      // step1 wait for ackFile in /tmp/ipc/ack/ackFile
      LOG.debug("samc: Waiting for ack File " + ackFile.toString() + "...");
      while (!ackFile.exists()) {
        Thread.sleep(100);
      }

      // step2 read the execute=true or false in ackFile
      LOG.debug("samc: Reading ack file " + ackFile.toString() + "...");
      fileReader = new FileReader(ackFileName);
      inRead = new BufferedReader(fileReader);
      String line;
      while ((line = inRead.readLine()) != null) {
        if (line.startsWith("execute=true")) {
          samcResponse = true;
        }
      }

      // step3 remove the /tmp/ipc/ack/ackFile
      LOG.debug("samc: Removing ack File " + ackFile.toString());
      ackFile.delete();

    } catch (InterruptedException e) {
      LOG.info("samc: Event Interceptor got interrupted", e);
    } catch (FileNotFoundException e) {
      LOG.error("samc: Ack file " + ackFile.toString() + " not found!", e);
    } catch (IOException e) {
      LOG.error("samc: Got error when reading ack file " + ackFile.toString(),
          e);
    }
    LOG.info("samc: control returned to SUT...");
  }

  public String getFileDir() {
    return fileDir;
  }

  public String createFileName() {
    long time = System.nanoTime();
    long postfix = time % 1000000;
    String filename =
        "hadoop-" + String.valueOf(eventId) + "-" + String.valueOf(postfix);
    return filename;
  }

  public int getEventId() {
    int prime = 19;
    int hash = 1;
    hash = prime * hash + sendNode;
    hash = prime * hash + recvNode;
    hash = prime * hash + nodeState;
    hash = prime * hash + eventTypeId;
    return hash;
  }

  public boolean hasSAMCResponse() {
    return samcResponse;
  }

  @Override
  public String toString() {
    return filename + "={sendNode: " + sendNode + ", recvNode: " + recvNode
        + ", interceptEventType: " + eventType.toString() + "}";
  }

  public void printToLog() {
    LOG.info("samc: " + this.toString());
  }
}
