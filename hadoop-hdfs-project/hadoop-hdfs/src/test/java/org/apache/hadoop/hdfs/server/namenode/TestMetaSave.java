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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the creation and validation of metasave
 */
public class TestMetaSave {
  public static final Log LOG = LogFactory.getLog(TestMetaSave.class);

  static final int NUM_DATA_NODES = 2;
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fileSys = null;
  private static NamenodeProtocols nnRpc = null;

  private void createFile(FileSystem fileSys, Path name) throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) 2, blockSize);
    byte[] buffer = new byte[1024];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }

  private void increaseReplica(String path) throws Exception {
    nnRpc.setReplication(path, (short) 4);
  }

  @Before
  public void setUp() throws IOException {
    // start a cluster
    Configuration conf = new HdfsConfiguration();

    // High value of replication interval
    // so that blocks remain under-replicated
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 7200);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 5L);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    nnRpc = cluster.getNameNodeRpc();
  }

  /**
   * Tests metasave
   */
  @Test
  public void testMetaSave()
      throws IOException, InterruptedException, TimeoutException {
    for (int i = 0; i < 2; i++) {
      Path file = new Path("/filestatus" + i);
      createFile(fileSys, file);
    }

    // stop datanode and wait for namenode to discover that a datanode is dead
    stopDatanodeAndWait(1);

    nnRpc.setReplication("/filestatus0", (short) 4);

    nnRpc.metaSave("metasave.out.txt");

    // Verification
    FileInputStream fstream = new FileInputStream(getLogFile(
      "metasave.out.txt"));
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(in));
      String line = reader.readLine();
      assertTrue(line.equals(
          "3 files and directories, 2 blocks = 5 total"));
      line = reader.readLine();
      assertTrue(line.equals("Live Datanodes: 1"));
      line = reader.readLine();
      assertTrue(line.equals("Dead Datanodes: 1"));
      reader.readLine();
      line = reader.readLine();
      assertTrue(line.matches("^/filestatus[01]:.*"));
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * Tests metasave after delete, to make sure there are no orphaned blocks
   */
  @Test
  public void testMetasaveAfterDelete()
      throws IOException, InterruptedException, TimeoutException {
    for (int i = 0; i < 2; i++) {
      Path file = new Path("/filestatus" + i);
      createFile(fileSys, file);
    }

    // stop datanode and wait for namenode to discover that a datanode is dead
    stopDatanodeAndWait(1);

    nnRpc.setReplication("/filestatus0", (short) 4);
    nnRpc.delete("/filestatus0", true);
    nnRpc.delete("/filestatus1", true);

    nnRpc.metaSave("metasaveAfterDelete.out.txt");

    // Verification
    BufferedReader reader = null;
    try {
      FileInputStream fstream = new FileInputStream(getLogFile(
        "metasaveAfterDelete.out.txt"));
      DataInputStream in = new DataInputStream(fstream);
      reader = new BufferedReader(new InputStreamReader(in));
      reader.readLine();
      String line = reader.readLine();
      assertTrue(line.equals("Live Datanodes: 1"));
      line = reader.readLine();
      assertTrue(line.equals("Dead Datanodes: 1"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks waiting for reconstruction: 0"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks currently missing: 0"));
      line = reader.readLine();
      assertTrue(line.equals("Mis-replicated blocks that have been postponed:"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks being replicated: 0"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks 2 waiting deletion from 1 datanodes."));
      //skip 2 lines to reach HDFS-9033 scenario.
      line = reader.readLine();
      line = reader.readLine();
      assertTrue(line.contains("blk"));
      // skip 1 line for Corrupt Blocks section.
      line = reader.readLine();
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Number of datanodes: 2"));
      line = reader.readLine();
      assertFalse(line.contains("NaN"));

    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * Tests that metasave overwrites the output file (not append).
   */
  @Test
  public void testMetaSaveOverwrite() throws Exception {
    // metaSave twice.
    nnRpc.metaSave("metaSaveOverwrite.out.txt");
    nnRpc.metaSave("metaSaveOverwrite.out.txt");

    // Read output file.
    FileInputStream fis = null;
    InputStreamReader isr = null;
    BufferedReader rdr = null;
    try {
      fis = new FileInputStream(getLogFile("metaSaveOverwrite.out.txt"));
      isr = new InputStreamReader(fis);
      rdr = new BufferedReader(isr);

      // Validate that file was overwritten (not appended) by checking for
      // presence of only one "Live Datanodes" line.
      boolean foundLiveDatanodesLine = false;
      String line = rdr.readLine();
      while (line != null) {
        if (line.startsWith("Live Datanodes")) {
          if (foundLiveDatanodesLine) {
            fail("multiple Live Datanodes lines, output file not overwritten");
          }
          foundLiveDatanodesLine = true;
        }
        line = rdr.readLine();
      }
    } finally {
      IOUtils.cleanup(null, rdr, isr, fis);
    }
  }

  class FileCreateTask implements Runnable {
    private Path file;
    private CountDownLatch latch;

    public FileCreateTask(Path file, CountDownLatch latch) {
      this.file = file;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        createFile(fileSys, file);
        latch.countDown();
      } catch (IOException e) {
        LOG.error("Failed to create file " + file);
      }
    }

  }

  class IncreaseReplicationTask implements Runnable {
    private String file;
    CountDownLatch latch;

    public IncreaseReplicationTask(String file, CountDownLatch latch) {
      this.file = file;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        increaseReplica(file);
        latch.countDown();
      } catch (Exception e) {
        LOG.error("Failed to increase replication of file " + file);
      }
    }

  }

  /**
   * Tests metasave performance when there are many under replicated blocks
   */
  @Test
  public void testMetaSaveWithLargeUnderReplicate() throws Exception {
    long maxNumFile = 16384;
    int maxPool = 100;
    ExecutorService pool = Executors.newFixedThreadPool(maxPool);
    CountDownLatch latch;

    long exponent = 1;
    long begin = 0;
    long end = (long) Math.pow(2, exponent);
    while (end < maxNumFile) {
      int totalFile = (int) (end - begin);
      latch = new CountDownLatch(totalFile);
      for (long i = begin; i < end; i++) {
        Path file = new Path("/file" + i);
        pool.submit(new FileCreateTask(file, latch));
      }
      try {
        latch.await();
      } catch (InterruptedException E) {
         LOG.error("File creation interrupted");
      }

      latch = new CountDownLatch(totalFile);
      for (long i = begin; i < end; i++) {
        pool.submit(new IncreaseReplicationTask("/file" + i, latch));
      }
      try {
        latch.await();
      } catch (InterruptedException E) {
         LOG.error("File replication increment interrupted");
      }

      long start = Time.monotonicNow();
      nnRpc.metaSave("metasave-" + end + ".out.txt");
      long elapsed = Time.monotonicNow() - start;
      LOG.info("metaSave elapsed time for " + end + " files is " + elapsed + " ms");

      exponent++;
      begin = end;
      end = Math.min(maxNumFile, (long) Math.pow(2, exponent));
    }

    pool.shutdown();
  }

  @After
  public void tearDown() throws IOException {
    if (fileSys != null)
      fileSys.close();
    if (cluster != null)
      cluster.shutdown();
  }

  /**
   * Returns a File for the given name inside the log directory.
   *
   * @param name String file name
   * @return File for given name inside log directory
   */
  private static File getLogFile(String name) {
    return new File(System.getProperty("hadoop.log.dir"), name);
  }

  /**
   * Stop a DN, notify NN the death of DN and wait for NN to remove the DN.
   *
   * @param dnIdx Index of the Datanode in MiniDFSCluster
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void stopDatanodeAndWait(final int dnIdx)
      throws TimeoutException, InterruptedException {
    final DataNode dnToStop = cluster.getDataNodes().get(dnIdx);
    cluster.stopDataNode(dnIdx);
    BlockManagerTestUtil.noticeDeadDatanode(
        cluster.getNameNode(), dnToStop.getDatanodeId().getXferAddr());
    // wait for namenode to discover that a datanode is dead
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return BlockManagerTestUtil.isDatanodeRemoved(
            cluster.getNameNode(), dnToStop.getDatanodeUuid());
      }
    }, 1000, 30000);
  }
}
