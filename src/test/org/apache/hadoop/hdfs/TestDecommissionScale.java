/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.Collection;
import java.util.Random;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.net.*;
import java.lang.InterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommissionScale extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 1024;
  static final int numDatanodes = 512;
  static final int numToDecom = 256;
  static final int fileSize = numToDecom*10*blockSize;
  static final int numFiles = 1000;
  static final int replicas = 3;

  class FileWriter implements Runnable {

    private Path filePath;
    private FileSystem fileSys;

    public FileWriter(FileSystem fileSys, Path var) {
      this.fileSys = fileSys;
      this.filePath = var;
    }

    public void run() {
      try {
        writeFile(fileSys, filePath, replicas);
        System.out.println("Created file "+filePath.toString()+" with " +
                           replicas + " replicas.");
        checkFile(fileSys, filePath, replicas);
      } catch (IOException ex) {
        System.out.println(filePath.toString()+" failed to be written: "+ex.getMessage());
      }
    }
  }


  Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;

  ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);

  private enum NodeState {NORMAL, DECOMMISSION_INPROGRESS, DECOMMISSIONED; }

  private void writeConfigFile(FileSystem fs, Path name, ArrayList<String> nodes) 
    throws IOException {

    // delete if it already exists
    if (fs.exists(name)) {
      fs.delete(name, true);
    }

    FSDataOutputStream stm = fs.create(name);
    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains (fileSize/blockSize) blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, 
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    //try {
    //  Thread.sleep(1000);
    //} catch (InterruptedException e) {
      // nothing
    //}
  }
  
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    DFSTestUtil.waitReplication(fileSys, name, (short) repl);
  }

  private void printFileLocations(FileSystem fileSys, Path name)
  throws IOException {
    BlockLocation[] locations = fileSys.getFileBlockLocations(
        fileSys.getFileStatus(name), 0, fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      if (idx > 10) continue;
      String[] loc = locations[idx].getNames();
      System.out.print("Block[" + idx + "] : ");
      for (int j = 0; j < loc.length; j++) {
        System.out.print(loc[j] + " ");
      }
      System.out.println("");
    }
  }

  /**
   * For blocks that reside on the nodes that are down, verify that their
   * replication factor is 1 more than the specified one.
   */
  private void checkFile(FileSystem fileSys, Path name, int repl,
                         String downnode) throws IOException {
    //
    // sleep an additional 10 seconds for the blockreports from the datanodes
    // to arrive. 
    //
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(), fileSys instanceof DistributedFileSystem);
        
    DFSClient.DFSDataInputStream dis = (DFSClient.DFSDataInputStream) 
      ((DistributedFileSystem)fileSys).open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();

    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) {     // for each replica
        if (nodes[j].getName().equals(downnode)) {
          hasdown++;
          System.out.println("Block " + blk.getBlock() + " replica " +
                             nodes[j].getName() + " is decommissioned.");
        }
      }
      System.out.println("Block " + blk.getBlock() + " has " + hasdown +
                         " decommissioned replica.");
      assertEquals("Number of replicas for block" + blk.getBlock(),
                   Math.min(numDatanodes, repl+hasdown), nodes.length);  
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    System.out.println("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      System.out.println(info[i].getDatanodeReport());
      System.out.println();
    }
  }


  /*
   * get list of living datanodes.
   */
  private ArrayList<String> getLivingNodes(NameNode namenode,
                                  Configuration conf,
                                  DFSClient client, 
                                  FileSystem filesys,
                                  FileSystem localFileSys)
    throws IOException {
    ArrayList<String> livingNodes = new ArrayList<String>();
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    System.out.println("We have "+info.length+" datanodes live"); 
    for (DatanodeInfo dn: info) {
      System.out.println("Live node "+dn.getName());
      livingNodes.add(dn.getName());
    }
    return livingNodes;
  }

  /*
   * decommission one random node.
   */
  private String decommissionNode(NameNode namenode,
                                  Configuration conf,
                                  DFSClient client, 
                                  FileSystem filesys,
                                  FileSystem localFileSys)
    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    //
    // pick one datanode randomly.
    //
    int index = 0;
    boolean found = false;
    while (!found) {
      index = myrand.nextInt(info.length);
      if (!info[index].isDecommissioned()) {
        found = true;
      }
    }
    String nodename = info[index].getName();
    System.out.println("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.add(nodename);
    writeConfigFile(localFileSys, excludeFile, nodes);
    namenode.namesystem.refreshNodes(conf);
    return nodename;
  }

  /*
   * decommission selected nodes.
   */
  private ArrayList<String> decommissionNodes(NameNode namenode,
                                  Configuration conf,
                                  DFSClient client, 
                                  FileSystem filesys,
                                  FileSystem localFileSys,
                                  ArrayList<String> toDecom)
    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.addAll(toDecom);
    writeConfigFile(localFileSys, excludeFile, nodes);
    namenode.namesystem.refreshNodes(conf);
    return toDecom;
  }

  /*
   * put node back in action
   */
  private void commissionNode(FileSystem filesys, FileSystem localFileSys,
                              String node) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;

    System.out.println("Commissioning nodes.");
    writeConfigFile(localFileSys, excludeFile, null);
    dfs.refreshNodes();
  }

  /*
   * Check if node is in the requested state.
   */
  private boolean checkNodeState(FileSystem filesys, 
                                 String node, 
                                 NodeState state) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    boolean done = false;
    boolean foundNode = false;
    DatanodeInfo[] datanodes = dfs.getDataNodeStats();
    for (int i = 0; i < datanodes.length; i++) {
      DatanodeInfo dn = datanodes[i];
      if (dn.getName().equals(node)) {
        if (state == NodeState.DECOMMISSIONED) {
          done = dn.isDecommissioned();
        } else if (state == NodeState.DECOMMISSION_INPROGRESS) {
          done = dn.isDecommissionInProgress();
        } else {
          done = (!dn.isDecommissionInProgress() && !dn.isDecommissioned());
        }
        System.out.println(dn.getDatanodeReport());
        foundNode = true;
      }
    }
    if (!foundNode) {
      throw new IOException("Could not find node: " + node);
    }
    return done;
  }

  /* 
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(FileSystem filesys,
                             String node,
                             NodeState state) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    boolean done = checkNodeState(filesys, node, state);
    while (!done) {
      System.out.println("Waiting for node " + node +
                         " to change state to " + state);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing
      }
      done = checkNodeState(filesys, node, state);
    }
  }


  /*
   * Wait till nodes are fully decommissioned.
   */
  private boolean waitNodesDecommissioned(FileSystem filesys, 
                                 ArrayList<String> nodes
                                 ) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    ArrayList<String> toDecom = new ArrayList<String>(nodes);
    boolean done = false;
    do {
      try {
        System.out.println("Waiting "+toDecom.size()+" datanodes to decommission");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing
        break;
      }

      done = true;
      DatanodeInfo[] datanodes = dfs.getDataNodeStats();
      for (int i = 0; i < datanodes.length; i++) {
        DatanodeInfo dn = datanodes[i];
        if (toDecom.contains(dn.getName())) {
          done = done && dn.isDecommissioned();
          if (dn.isDecommissioned()) {
            System.out.println(dn.getDatanodeReport());
            toDecom.remove(dn.getName());
          }
        }
      }
    } while (!done);
    return done;
  }
  
  /**
   * Tests Decommission in DFS.
   */
  public void testDecommission() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.replication.considerLoad", false);

    // Set up the hosts/exclude files.
    FileSystem localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, "build/test/data/work-dir/decommission");
    assertTrue(localFileSys.mkdirs(dir));
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());
    conf.setInt("heartbeat.recheck.interval", 2000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 4);
    //conf.setInt("dfs.namenode.decommission.interval", 30);
    writeConfigFile(localFileSys, excludeFile, null);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) fileSys;

    try {
      ArrayList<String> allNodes = getLivingNodes(cluster.getNameNode(), conf,
                                                   client, fileSys, localFileSys);
      ArrayList<String> downnodes = new ArrayList<String>();
      ArrayList<MiniDFSCluster.DataNodeProperties> paused = new ArrayList<MiniDFSCluster.DataNodeProperties>();

      for (int i=numDatanodes-1; i>=numToDecom ; i--) {
        System.out.println("Stopping datanode "+allNodes.get(i)+" ...");
        paused.add(cluster.stopDataNode(i));
      }

      Thread.sleep(15000);
      ((DistributedFileSystem) fileSys).refreshNodes();
      downnodes = getLivingNodes(cluster.getNameNode(), conf,
                                 client, fileSys, localFileSys);

      ArrayList<Path> files = new ArrayList<Path>();

      ExecutorService executor = Executors.newFixedThreadPool(32);
      for (int i=0; i<numFiles; i++) {
        Path file1 = new Path("inflight-"+i+".dat");
        Runnable worker = new FileWriter(fileSys,file1);
        executor.execute(worker);
      }
      executor.shutdown();
      while (!executor.isTerminated()) {
        executor.awaitTermination(1,TimeUnit.HOURS);
      }

      /*for (int i=0; i<numFiles; i++) {
        Path file1 = new Path("decommission-"+i+".dat");
        writeFile(fileSys, file1, replicas);
        System.out.println("Created file decommission-"+i+".dat with " +
                           replicas + " replicas.");
        checkFile(fileSys, file1, replicas);
        //printFileLocations(fileSys, file1);
      }*/

      for (MiniDFSCluster.DataNodeProperties dp: paused) {
        System.out.println("Restarting datanode "+dp.datanode.dnRegistration.getName()+" ...");
        cluster.restartDataNode(dp);
      }

      ((DistributedFileSystem) fileSys).refreshNodes();
      //cluster.getNameNode().namesystem.setDecommissionHack(true);
      //Thread.sleep(10000);
      allNodes = getLivingNodes(cluster.getNameNode(), conf,
                                client, fileSys, localFileSys);

      downnodes = decommissionNodes(cluster.getNameNode(), conf,
                                    client, fileSys, localFileSys, downnodes);
      decommissionedNodes.addAll(downnodes);

      waitNodesDecommissioned(fileSys, downnodes);
      //checkFile(fileSys, file1, replicas, downnode);
      for (Path file1:files)
        cleanupFile(fileSys, file1);
      cleanupFile(localFileSys, dir);
    } catch (IOException e) {
      info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
