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
public class VerificationNotifier extends Thread {

  private static final Log LOG = LogFactory.getLog(VerificationNotifier.class);

  Configuration conf;

  int sendNode;
  String key;
  String value;

  String fileDir;
  String filename;
  File eventFile;

  boolean samcResponse;

  public VerificationNotifier(NodeRole roleS, String key, String value) {
    conf = new YarnConfiguration();

    this.sendNode = roleS.ordinal();
    this.key = key;
    this.value = value;
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
      writer.print("key=" + key + "\n");
      writer.print("value=" + value + "\n");
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
        "hadoopVerify-" + key + "-" + value + "-" + String.valueOf(postfix);
    return filename;
  }

  @Override
  public String toString() {
    return filename + "={sendNode: " + sendNode + ", " + key + ": " + value
        + "}";
  }

  public void printToLog() {
    LOG.info("samc: " + this.toString());
  }
}
