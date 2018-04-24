package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.util.Time;

public class TestSnapshotDiffReportScale {
  private static final Log LOG = LogFactory.getLog(TestSnapshotDiffReportScale.class);

  protected static final long seed = 0;
  protected static final short REPLICATION = 3;
  protected static final short REPLICATION_1 = 2;
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;

  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem hdfs;

  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(IPC_MAXIMUM_DATA_LENGTH, 512 * 1024 * 1024);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
    LOG.info("MiniDFSCluster started with following conf\n" + conf.toString());
  }

  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /** just simply make diff report */
  private void callDiffReport(Path dir, String from, String to) throws IOException {
    LOG.info("diff calculation between " + from + " and " + to + " start.");
    long start = Time.monotonicNow();
    SnapshotDiffReport report = hdfs.getSnapshotDiffReport(dir, from, to);
    long diff = Time.monotonicNow() - start;
    LOG.info("diff calculation between " + from + " and " + to + " finish.");
    // System.out.println(report);
    LOG.info(String.format(
        "getSnapshotDiffReport between " + from + " and " + to
            + " took %d ms and contain %d diff",
        diff, report.getDiffList().size()));
  }
  
  private void makeSnapshot(DistributedFileSystem hdfs,
      Path snapshotRoot, String snapshotName) throws Exception {
    LOG.info("making of snapshot " + snapshotName + " start.");
    long start = Time.monotonicNow();
    SnapshotTestHelper.createSnapshot(hdfs, snapshotRoot, snapshotName);
    long diff = Time.monotonicNow() - start;
    LOG.info("making of snapshot " + snapshotName + " finish.");
    LOG.info("snapshot " + snapshotName + " creation took " + diff + " ms");
  }

  /**
   * Create many directories and measure its diff time
   */
  public void testDiffReportWithMillionCreate() throws Exception {
    final int numL1 = 17;
    final int numL2 = 1000;
    final int numL3 = 1000;
    final Path root = new Path("/");
    final Path tdir = new Path(root, "td");
    final String subdirPattern = "%03d";

    // create initial subdirs
    for (int i=0; i<numL1; i++) {
      Path subDir = new Path(tdir, String.format(subdirPattern, i));
      for (int j=0; j<numL2; j++) {
        Path subsubDir = new Path(subDir, String.format(subdirPattern, j));
        hdfs.mkdirs(subsubDir);
        LOG.info("Path " + subsubDir + " created");
      }
    }

    // create initial snapshot on root
    String intialSnapshot = "s0";
    makeSnapshot(hdfs, root, intialSnapshot);

    int n = 1;
    long nextSnapshot = (long) Math.pow(2, n);
    long ct = 0;
    
    // change subdirs permission
    for (int i=0; i<numL1; i++) {
      Path subDir = new Path(tdir, String.format(subdirPattern, i));
      for (int j=0; j<numL2; j++) {
        Path subsubDir = new Path(subDir, String.format(subdirPattern, j));
        for (int k=0; k<numL3; k++) {
          Path subsubsubDir = new Path(subsubDir, String.format(subdirPattern, k));
          hdfs.mkdirs(subsubsubDir);
          ct++;
          
          if (ct == nextSnapshot) {
            // create snapshot n
            String snapshotName = "s"+n;
            makeSnapshot(hdfs, root, snapshotName);
            
            // measure diff time
            callDiffReport(root, intialSnapshot, snapshotName);
            nextSnapshot = (long) Math.pow(2, ++n);
          }
        }
        LOG.info("Path " + subsubDir + " updated");
      }
    }
  }

  public static void main(String[] args) {
    TestSnapshotDiffReportScale tsdr = new TestSnapshotDiffReportScale();
    try {
      tsdr.setUp();
      tsdr.testDiffReportWithMillionCreate();
      LOG.info("TestSnapshotDiffReportScale is done");
    } catch (Exception ex) {
      LOG.error(ExceptionUtils.getFullStackTrace(ex));
    } finally {
      tsdr.tearDown();
    }
  }

}
