package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by huanke on 11/27/16.
 */
public class RMEventInterceptor {
  private static final Log LOG = LogFactory.getLog(RMEventInterceptor.class);
  int nodeId;
  int nodeType;

  int sendNode;
  int recvNode;
  int nodeState; // 1 represents alived and o represents crashed.
  RMInterceptEventType interceptEventType;
  int interceptEventTypeId;
  int eventId;

  String fileDir;
  String filename;

  boolean samcResponse;

  public RMEventInterceptor(String ipcDir, Role roleS, Role roleR,
      int nodeState, RMInterceptEventType interceptEventType) {
    this.sendNode = getRoleId(roleS);
    this.recvNode = getRoleId(roleR);
    this.nodeState = nodeState;
    this.interceptEventType = interceptEventType;
    interceptEventTypeId = getTypeId(interceptEventType);
    eventId = getEventId();
    fileDir = ipcDir;
    filename = createFileName();
    samcResponse = false;

    // begin to write /new/filename
    try {
      String newFileName = fileDir + "/new/" + filename;
      PrintWriter writer = new PrintWriter(newFileName, "UTF-8");
      writer.print("sendNode=" + this.sendNode + "\n");
      writer.print("recvNode=" + this.recvNode + "\n");
      writer.print("nodeState=" + this.nodeState + "\n");
      writer.print("eventTypeId=" + interceptEventTypeId + "\n");
      writer.print("eventType=" + this.interceptEventType.toString() + "\n");
      writer.print("eventId=" + eventId);
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    commitEvent();
    waitAck();
  }

  public int getRoleId(Role role) {
    switch (role) {
    case RM:
      this.nodeId = 1;
      break;
    case NM:
      this.nodeId = 2;
      break;
    case AM:
      this.nodeId = 3;
      break;
    default:
      this.nodeId = 0;
      break;
    }
    return this.nodeId;
  }

  public int getTypeId(RMInterceptEventType interceptEventType) {
    switch (interceptEventType) {
    case AMLauncheee:
      this.nodeType = 1;
      break;
    case AMCleanup:
      this.nodeType = 2;
      break;
    default:
      this.nodeType = 0;
      break;
    }
    return this.nodeType;
  }

  public void commitEvent() {
    try {
      String moveFile = "mv " + fileDir + "/new/" + filename + " " + fileDir
          + "/send/" + filename;
      Process p = Runtime.getRuntime().exec(moveFile);
      p.waitFor();
      LOG.info("@HK " + moveFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void waitAck() {
    String ackFileName = fileDir + "/ack/" + filename;
    FileReader fileReader = null;
    BufferedReader inRead = null;
    File ackFile = new File(ackFileName);

    // step1 wait for ackFile in /tmp/ipc/ack/ackFile
    LOG.info("@HK Step1 -> waiting for ack File " + ackFileName + "...");
    while (!ackFile.exists()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // step2 read the execute=true or false in ackFile
    try {
      fileReader = new FileReader(ackFileName);
      inRead = new BufferedReader(fileReader);
      String line;
      try {
        while ((line = inRead.readLine()) != null) {
          LOG.info("@HK " + line);
          if (line.equals("execute=true")) {
            samcResponse = true;
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    LOG.info("@HK Step2 -> read ack File " + ackFileName);

    // step3 remove the /tmp/ipc/ack/ackFile
    try {
      Runtime.getRuntime().exec("rm -r " + fileDir + "/ack/" + filename);
    } catch (IOException e) {
      e.printStackTrace();
    }
    LOG.info("@HK Step3 -> remove ack File " + ackFileName);
  }

  public String getFileDir() {
    return fileDir;
  }

  public String createFileName() {
    long time = System.currentTimeMillis();
    int count = (int) (time % 10000);
    String filename =
        "hadoop-" + String.valueOf(eventId) + "-" + String.valueOf(count);
    LOG.info("@HK " + filename);
    return filename;
  }

  public int getEventId() {
    int prime = 19;
    int hash = 1;
    hash = prime * hash + this.sendNode;
    hash = prime * hash + this.recvNode;
    hash = prime * hash + this.interceptEventTypeId;
    return hash;
  }

  public boolean getSAMCResponse() {
    return samcResponse;
  }

  public void printString() {
    LOG.info("@HK sendNode: " + this.sendNode + " recvNode: " + this.recvNode
        + " interceptEventType: " + this.interceptEventType.toString());
  }
}
