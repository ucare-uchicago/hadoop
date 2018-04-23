package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /** just simply make diff report */
  private void callDiffReport(Path dir, String from, String to) throws IOException {
    long start = Time.monotonicNow();
    SnapshotDiffReport report = hdfs.getSnapshotDiffReport(dir, from, to);
    long diff = Time.monotonicNow() - start;
    // System.out.println(report);
    LOG.info(
        String.format("getSnapshotDiffReport took %d ms and contain %d diff",
            diff, report.getDiffList().size()));
  }

  /**
   * Create many directories and measure its diff time
   */
  public void testDiffReportWithMillionCreate() throws Exception {
    final int numL1 = 20;
    final int numL2 = 1000;
    final int numL3 = 1000;
    final Path root = new Path("/");
    final Path tdir = new Path(root, "td");
    final String subdirPattern = "%03d";
    final String leafPattern = "L%03d";

    // create initial subdirs
    for (int i=0; i<numL1; i++) {
      Path subDir = new Path(tdir, String.format(subdirPattern, i));
      for (int j=0; j<numL2; j++) {
        Path subsubDir = new Path(subDir, String.format(subdirPattern, j));
        for (int k=0; k<numL3; k++) {
          Path subsubsubDir = new Path(subsubDir, String.format(subdirPattern, k));
          hdfs.mkdirs(subsubsubDir);
        }
	LOG.info("Path " + subsubDir + " created");
      }
    }

    // create snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, "s1");

    // change subdirs permission
    for (int i=0; i<numL1; i++) {
      Path subDir = new Path(tdir, String.format(subdirPattern, i));
      for (int j=0; j<numL2; j++) {
        Path subsubDir = new Path(subDir, String.format(subdirPattern, j));
        for (int k=0; k<numL3; k++) {
          Path subsubsubDir = new Path(subsubDir, String.format(subdirPattern, k));
          FsPermission perm = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ);
          hdfs.setPermission(subsubsubDir, perm);
        }
        LOG.info("Path " + subsubDir + " updated");
      }
    }

    // snapshot again
    SnapshotTestHelper.createSnapshot(hdfs, root, "s2");
    // let's delete /dir2 to make things more complicated
    hdfs.delete(tdir, true);

    callDiffReport(root, "s1", "s2");
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
