package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.util.Time;

public class TestMetaSaveScale {
  public static final Log LOG = LogFactory.getLog(TestMetaSaveScale.class);

  static final int NUM_DATA_NODES = 4;
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 1024;
  static final int maxNumFile = 16777216;
  private MiniDFSCluster cluster = null;
  private FileSystem fileSys = null;
  private NamenodeProtocols nnRpc = null;
  private BlockManager bm = null;

  public void setUp() throws IOException {
    // start a cluster
    Configuration conf = new HdfsConfiguration();

    // High value of replication interval
    // so that blocks remain under-replicated
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 7200);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1L);
    // set high stream value
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, maxNumFile);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, maxNumFile);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY, maxNumFile);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    nnRpc = cluster.getNameNodeRpc();
    bm = cluster.getNameNode().getNamesystem().getBlockManager();
  }

  public void tearDown() {
    try {
      if (fileSys != null)
        fileSys.close();
      if (cluster != null)
        cluster.shutdown();
    } catch (IOException ex) {
      LOG.error("Got exception when tearing down\n"
          + ExceptionUtils.getFullStackTrace(ex));
    }
  }

  private void increaseReplica(String path) throws Exception {
    nnRpc.setReplication(path, (short) 2);
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
        DFSTestUtil.createFile(fileSys, file, blockSize, (short) 1, seed);
      } catch (IOException e) {
        LOG.error("Failed to create file " + file);
      } finally {
        latch.countDown();
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
      } catch (Exception e) {
        LOG.error("Failed to increase replication of file " + file);
      } finally {
        latch.countDown();
      }
    }

  }

  public String toPath(int numFile) {
    int fileNum = numFile;
    int subsubdir = (numFile / 1000) % 1000;
    int subdir = (numFile / 1000000);
    return String.format("/test/%d/%d/file-%d", subdir, subsubdir, fileNum);
  }

  /**
   * Tests metasave performance when there are many under replicated blocks
   */
  public void testMetaSaveWithLargeUnderReplicate() throws Exception {
    int maxPool = NUM_DATA_NODES * 4;
    ExecutorService pool = Executors.newFixedThreadPool(maxPool);
    CountDownLatch latch;

    int exponent = 1;
    int begin = 0;
    int end = (int) Math.pow(2, exponent);
    while (end <= maxNumFile) {
      int totalFile = (end - begin);
      latch = new CountDownLatch(totalFile);
      for (int i = begin; i < end; i++) {
        Path file = new Path(toPath(i));
        pool.submit(new FileCreateTask(file, latch));
      }
      try {
        latch.await();
      } catch (InterruptedException E) {
         LOG.error("File creation interrupted");
      }

      latch = new CountDownLatch(totalFile);
      for (int i = begin; i < end; i++) {
        pool.submit(new IncreaseReplicationTask(toPath(i), latch));
      }
      try {
        latch.await();
      } catch (InterruptedException E) {
         LOG.error("File replication increment interrupted");
      }

      //long bmStart = Time.monotonicNow();
      //BlockManagerTestUtil.computeAllPendingWork(bm);
      //long bmElapsed = Time.monotonicNow() - bmStart;
      //LOG.info("BlockManagerTestUtil.computeAllPendingWork took " + bmElapsed + " ms");

      long start = Time.monotonicNow();
      nnRpc.metaSave("metasave-" + end + ".out.txt");
      long elapsed = Time.monotonicNow() - start;
      LOG.info("metaSave elapsed time for " + end + " files is " + elapsed + " ms");

      exponent++;
      begin = end;
      end = Math.min(maxNumFile+1, (int) Math.pow(2, exponent));
    }

    pool.shutdown();
  }

  public static void main(String[] args) {
    TestMetaSaveScale tsdr = new TestMetaSaveScale();
    try {
      tsdr.setUp();
      tsdr.testMetaSaveWithLargeUnderReplicate();
      LOG.info("TestSnapshotDiffReportScale is done");
    } catch (Exception ex) {
      LOG.error(ExceptionUtils.getFullStackTrace(ex));
    } finally {
      tsdr.tearDown();
    }
  }

}
