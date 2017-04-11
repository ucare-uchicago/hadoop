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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SimpleStat;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * Main class for a series of name-node benchmarks.
 * 
 * Each benchmark measures throughput and average execution time 
 * of a specific name-node operation, e.g. file creation or block reports.
 * 
 * The benchmark does not involve any other hadoop components
 * except for the name-node. Each operation is executed
 * by calling directly the respective name-node method.
 * The name-node here is real all other components are simulated.
 * 
 * Command line arguments for the benchmark include:
 * <ol>
 * <li>total number of operations to be performed,</li>
 * <li>number of threads to run these operations,</li>
 * <li>followed by operation specific input parameters.</li>
 * <li>-logLevel L specifies the logging level when the benchmark runs.
 * The default logging level is {@link Level#ERROR}.</li>
 * <li>-UGCacheRefreshCount G will cause the benchmark to call
 * {@link NameNodeRpcServer#refreshUserToGroupsMappings} after
 * every G operations, which purges the name-node's user group cache.
 * By default the refresh is never called.</li>
 * <li>-keepResults do not clean up the name-space after execution.</li>
 * <li>-useExisting do not recreate the name-space, use existing data.</li>
 * </ol>
 * 
 * The benchmark first generates inputs for each thread so that the
 * input generation overhead does not effect the resulting statistics.
 * The number of operations performed by threads is practically the same. 
 * Precisely, the difference between the number of operations 
 * performed by any two threads does not exceed 1.
 * 
 * Then the benchmark executes the specified number of operations using 
 * the specified number of threads and outputs the resulting stats.
 */
public class NNThroughputBenchmark implements Tool {
  private static final Log LOG = LogFactory.getLog(NNThroughputBenchmark.class);
  private static final int BLOCK_SIZE = 16;
  private static final String GENERAL_OPTIONS_USAGE = 
    "     [-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G]";

  static Configuration config;
  static NameNode nameNode;
  static NamenodeProtocols nameNodeProto;
  
  // GEDA module
  static boolean isGEDAmode;

  NNThroughputBenchmark(Configuration conf) throws IOException {
    config = conf;
    // We do not need many handlers, since each thread simulates a handler
    // by calling name-node methods directly
    config.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    // Turn off minimum block size verification
    config.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    // set exclude file
    config.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE,
      "${hadoop.tmp.dir}/dfs/hosts/exclude");
    File excludeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE,
      "exclude"));
    if(!excludeFile.exists()) {
      if(!excludeFile.getParentFile().exists() && !excludeFile.getParentFile().mkdirs())
        throw new IOException("NNThroughputBenchmark: cannot mkdir " + excludeFile);
    }
    new FileOutputStream(excludeFile).close();
    // set include file
    config.set(DFSConfigKeys.DFS_HOSTS, "${hadoop.tmp.dir}/dfs/hosts/include");
    File includeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS, "include"));
    new FileOutputStream(includeFile).close();
  }

  void close() {
    if(nameNode != null)
      nameNode.stop();
  }

  static void setNameNodeLoggingLevel(Level logLevel) {
    LOG.fatal("Log level = " + logLevel.toString());
    // change log level to NameNode logs
    DFSTestUtil.setNameNodeLogLevel(logLevel);
    GenericTestUtils.setLogLevel(LogManager.getLogger(
            NetworkTopology.class.getName()), logLevel);
    GenericTestUtils.setLogLevel(LogManager.getLogger(
            Groups.class.getName()), logLevel);
  }

  /**
   * Base class for collecting operation statistics.
   * 
   * Overload this class in order to run statistics for a 
   * specific name-node operation.
   */
  abstract class OperationStatsBase {
    protected static final String BASE_DIR_NAME = "/nnThroughputBenchmark";
    protected static final String OP_ALL_NAME = "all";
    protected static final String OP_ALL_USAGE = "-op all <other ops options>";

    protected final String baseDir;
    protected short replication;
    protected int  numThreads = 0;        // number of threads
    protected int  numOpsRequired = 0;    // number of operations requested
    protected int  numOpsExecuted = 0;    // number of operations executed
    protected long cumulativeTime = 0;    // sum of times for each op
    protected long elapsedTime = 0;       // time from start to finish
    protected boolean keepResults = false;// don't clean base directory on exit
    protected Level logLevel;             // logging level, ERROR by default
    protected int ugcRefreshCount = 0;    // user group cache refresh count

    protected List<StatsDaemon> daemons;

    /**
     * Operation name.
     */
    abstract String getOpName();

    /**
     * Parse command line arguments.
     * 
     * @param args arguments
     * @throws IOException
     */
    abstract void parseArguments(List<String> args) throws IOException;

    /**
     * Generate inputs for each daemon thread.
     * 
     * @param opsPerThread number of inputs for each thread.
     * @throws IOException
     */
    abstract void generateInputs(int[] opsPerThread) throws IOException;

    /**
     * This corresponds to the arg1 argument of 
     * {@link #executeOp(int, int, String)}, which can have different meanings
     * depending on the operation performed.
     * 
     * @param daemonId id of the daemon calling this method
     * @return the argument
     */
    abstract String getExecutionArgument(int daemonId);

    /**
     * Execute name-node operation.
     * 
     * @param daemonId id of the daemon calling this method.
     * @param inputIdx serial index of the operation called by the deamon.
     * @param arg1 operation specific argument.
     * @return time of the individual name-node call.
     * @throws IOException
     */
    abstract long executeOp(int daemonId, int inputIdx, String arg1) throws IOException;

    /**
     * Print the results of the benchmarking.
     */
    abstract void printResults();

    OperationStatsBase() {
      baseDir = BASE_DIR_NAME + "/" + getOpName();
      replication = (short) config.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
      numOpsRequired = 10;
      numThreads = 3;
      logLevel = Level.ERROR;
      ugcRefreshCount = Integer.MAX_VALUE;
      isGEDAmode = false;
    }

    void benchmark() throws IOException {
      daemons = new ArrayList<StatsDaemon>();
      long start = 0;
      try {
        numOpsExecuted = 0;
        cumulativeTime = 0;
        if(numThreads < 1)
          return;
        int tIdx = 0; // thread index < nrThreads
        int opsPerThread[] = new int[numThreads];
        for(int opsScheduled = 0; opsScheduled < numOpsRequired; 
                                  opsScheduled += opsPerThread[tIdx++]) {
          // execute  in a separate thread
          opsPerThread[tIdx] = (numOpsRequired-opsScheduled)/(numThreads-tIdx);
          if(opsPerThread[tIdx] == 0)
            opsPerThread[tIdx] = 1;
        }
        // if numThreads > numOpsRequired then the remaining threads will do nothing
        for(; tIdx < numThreads; tIdx++)
          opsPerThread[tIdx] = 0;
        setNameNodeLoggingLevel(Level.ERROR);
        generateInputs(opsPerThread);
        setNameNodeLoggingLevel(logLevel);
        for(tIdx=0; tIdx < numThreads; tIdx++)
          daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));
        start = Time.now();
        LOG.info("Starting " + numOpsRequired + " " + getOpName() + "(s).");
        for(StatsDaemon d : daemons)
          d.start();
      } finally {
        while(isInPorgress()) {
          // try {Thread.sleep(500);} catch (InterruptedException e) {}
        }
        elapsedTime = Time.now() - start;
        for(StatsDaemon d : daemons) {
          incrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
          // System.out.println(d.toString() + ": ops Exec = " + d.localNumOpsExecuted);
        }
      }
    }

    private boolean isInPorgress() {
      for(StatsDaemon d : daemons)
        if(d.isInProgress())
          return true;
      return false;
    }

    void cleanUp() throws IOException {
      nameNodeProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      if(!keepResults)
        nameNodeProto.delete(getBaseDir(), true);
    }

    int getNumOpsExecuted() {
      return numOpsExecuted;
    }

    long getCumulativeTime() {
      return cumulativeTime;
    }

    long getElapsedTime() {
      return elapsedTime;
    }

    long getAverageTime() {
      return numOpsExecuted == 0 ? 0 : cumulativeTime / numOpsExecuted;
    }

    double getOpsPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numOpsExecuted / elapsedTime;
    }

    String getBaseDir() {
      return baseDir;
    }

    String getClientName(int idx) {
      return getOpName() + "-client-" + idx;
    }

    void incrementStats(int ops, long time) {
      numOpsExecuted += ops;
      cumulativeTime += time;
    }

    /**
     * Parse first 2 arguments, corresponding to the "-op" option.
     * 
     * @param args argument list
     * @return true if operation is all, which means that options not related
     * to this operation should be ignored, or false otherwise, meaning
     * that usage should be printed when an unrelated option is encountered.
     */
    protected boolean verifyOpArgument(List<String> args) {
      if(args.size() < 2 || ! args.get(0).startsWith("-op"))
        printUsage();

      // process common options
      int krIndex = args.indexOf("-keepResults");
      keepResults = (krIndex >= 0);
      if(keepResults) {
        args.remove(krIndex);
      }

      int llIndex = args.indexOf("-logLevel");
      if(llIndex >= 0) {
        if(args.size() <= llIndex + 1)
          printUsage();
        logLevel = Level.toLevel(args.get(llIndex+1), Level.ERROR);
        args.remove(llIndex+1);
        args.remove(llIndex);
      }

      int ugrcIndex = args.indexOf("-UGCacheRefreshCount");
      if(ugrcIndex >= 0) {
        if(args.size() <= ugrcIndex + 1)
          printUsage();
        int g = Integer.parseInt(args.get(ugrcIndex+1));
        if(g > 0) ugcRefreshCount = g;
        args.remove(ugrcIndex+1);
        args.remove(ugrcIndex);
      }

      String type = args.get(1);
      if(OP_ALL_NAME.equals(type)) {
        type = getOpName();
        return true;
      }
      if(!getOpName().equals(type))
        printUsage();
      return false;
    }

    void printStats() {
      LOG.info("--- " + getOpName() + " stats  ---");
      LOG.info("# operations: " + getNumOpsExecuted());
      LOG.info("Elapsed Time: " + getElapsedTime());
      LOG.info(" Ops per sec: " + getOpsPerSecond());
      LOG.info("Average Time: " + getAverageTime());
    }
  }

  /**
   * One of the threads that perform stats operations.
   */
  private class StatsDaemon extends Thread {
    private final int daemonId;
    private int opsPerThread;
    private String arg1;      // argument passed to executeOp()
    private volatile int  localNumOpsExecuted = 0;
    private volatile long localCumulativeTime = 0;
    private final OperationStatsBase statsOp;

    StatsDaemon(int daemonId, int nrOps, OperationStatsBase op) {
      this.daemonId = daemonId;
      this.opsPerThread = nrOps;
      this.statsOp = op;
      setName(toString());
    }

    @Override
    public void run() {
      localNumOpsExecuted = 0;
      localCumulativeTime = 0;
      arg1 = statsOp.getExecutionArgument(daemonId);
      try {
        benchmarkOne();
      } catch(IOException ex) {
        LOG.error("StatsDaemon " + daemonId + " failed: \n" 
            + StringUtils.stringifyException(ex));
      }
    }

    @Override
    public String toString() {
      return "StatsDaemon-" + daemonId;
    }

    void benchmarkOne() throws IOException {
      for(int idx = 0; idx < opsPerThread; idx++) {
        if((localNumOpsExecuted+1) % statsOp.ugcRefreshCount == 0)
          nameNodeProto.refreshUserToGroupsMappings();
        long stat = statsOp.executeOp(daemonId, idx, arg1);
        localNumOpsExecuted++;
        localCumulativeTime += stat;
      }
    }

    boolean isInProgress() {
      return localNumOpsExecuted < opsPerThread;
    }

    /**
     * Schedule to stop this daemon.
     */
    void terminate() {
      opsPerThread = localNumOpsExecuted;
    }
  }

  /**
   * Clean all benchmark result directories.
   */
  class CleanAllStats extends OperationStatsBase {
    // Operation types
    static final String OP_CLEAN_NAME = "clean";
    static final String OP_CLEAN_USAGE = "-op clean";

    CleanAllStats(List<String> args) {
      super();
      parseArguments(args);
      numOpsRequired = 1;
      numThreads = 1;
      keepResults = true;
    }

    @Override
    String getOpName() {
      return OP_CLEAN_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      if(args.size() > 2 && !ignoreUnrelatedOptions)
        printUsage();
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      // do nothing
    }

    /**
     * Does not require the argument
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return null;
    }

    /**
     * Remove entire benchmark directory.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      nameNodeProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      long start = Time.now();
      nameNodeProto.delete(BASE_DIR_NAME, true);
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("Remove directory " + BASE_DIR_NAME);
      printStats();
    }
  }

  /**
   * File creation statistics.
   * 
   * Each thread creates the same (+ or -1) number of files.
   * File names are pre-generated during initialization.
   * The created files do not have blocks.
   */
  class CreateFileStats extends OperationStatsBase {
    // Operation types
    static final String OP_CREATE_NAME = "create";
    static final String OP_CREATE_USAGE = 
      "-op create [-threads T] [-files N] [-filesPerDir P] [-close]";

    protected FileNameGenerator nameGenerator;
    protected String[][] fileNames;
    private boolean closeUponCreate;

    CreateFileStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_CREATE_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrFilesPerDir = 4;
      closeUponCreate = false;
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-files")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-threads")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-filesPerDir")) {
          if(i+1 == args.size())  printUsage();
          nrFilesPerDir = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-close")) {
          closeUponCreate = true;
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileNameGenerator(getBaseDir(), nrFilesPerDir);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length"; 
      nameNodeProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      // int generatedFileIdx = 0;
      LOG.info("Generate " + numOpsRequired + " intputs for " + getOpName());
      fileNames = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        fileNames[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          fileNames[idx][jdx] = nameGenerator.
                                  getNextFileName("ThroughputBench");
      }
    }

    void dummyActionNoSynch(int daemonId, int fileIdx) {
      for(int i=0; i < 2000; i++)
        fileNames[daemonId][fileIdx].contains(""+i);
    }

    /**
     * returns client name
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do file create.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName) 
    throws IOException {
      long start = Time.now();
      // dummyActionNoSynch(fileIdx);
      nameNodeProto.create(fileNames[daemonId][inputIdx], FsPermission.getDefault(),
                      clientName, new EnumSetWritable<CreateFlag>(EnumSet
              .of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, 
          replication, BLOCK_SIZE, null);
      long end = Time.now();
      for(boolean written = !closeUponCreate; !written; 
        written = nameNodeProto.complete(fileNames[daemonId][inputIdx],
                                    clientName, null, INodeId.GRANDFATHER_INODE_ID));
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrFiles = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrFilesPerDir = " + nameGenerator.getFilesPerDirectory());
      printStats();
    }
  }

  /**
   * Directory creation statistics.
   *
   * Each thread creates the same (+ or -1) number of directories.
   * Directory names are pre-generated during initialization.
   */
  class MkdirsStats extends OperationStatsBase {
    // Operation types
    static final String OP_MKDIRS_NAME = "mkdirs";
    static final String OP_MKDIRS_USAGE = "-op mkdirs [-threads T] [-dirs N] " +
        "[-dirsPerDir P]";

    protected FileNameGenerator nameGenerator;
    protected String[][] dirPaths;

    MkdirsStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_MKDIRS_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrDirsPerDir = 2;
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-dirs")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-threads")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-dirsPerDir")) {
          if(i+1 == args.size())  printUsage();
          nrDirsPerDir = Integer.parseInt(args.get(++i));
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileNameGenerator(getBaseDir(), nrDirsPerDir);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length";
      nameNodeProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      LOG.info("Generate " + numOpsRequired + " inputs for " + getOpName());
      dirPaths = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        dirPaths[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          dirPaths[idx][jdx] = nameGenerator.
              getNextFileName("ThroughputBench");
      }
    }

    /**
     * returns client name
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do mkdirs operation.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName)
        throws IOException {
      long start = Time.now();
      nameNodeProto.mkdirs(dirPaths[daemonId][inputIdx],
          FsPermission.getDefault(), true);
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrDirs = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrDirsPerDir = " + nameGenerator.getFilesPerDirectory());
      printStats();
    }
  }

  /**
   * Open file statistics.
   * 
   * Measure how many open calls (getBlockLocations()) 
   * the name-node can handle per second.
   */
  class OpenFileStats extends CreateFileStats {
    // Operation types
    static final String OP_OPEN_NAME = "open";
    static final String OP_USAGE_ARGS = 
      " [-threads T] [-files N] [-filesPerDir P] [-useExisting]";
    static final String OP_OPEN_USAGE = 
      "-op " + OP_OPEN_NAME + OP_USAGE_ARGS;

    private boolean useExisting;  // do not generate files, use existing ones

    OpenFileStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_OPEN_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      int ueIndex = args.indexOf("-useExisting");
      useExisting = (ueIndex >= 0);
      if(useExisting) {
        args.remove(ueIndex);
      }
      super.parseArguments(args);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      // create files using opsPerThread
      String[] createArgs = new String[] {
              "-op", "create", 
              "-threads", String.valueOf(this.numThreads), 
              "-files", String.valueOf(numOpsRequired),
              "-filesPerDir", 
              String.valueOf(nameGenerator.getFilesPerDirectory()),
              "-close"};
      CreateFileStats opCreate =  new CreateFileStats(Arrays.asList(createArgs));

      if(!useExisting) {  // create files if they were not created before
        opCreate.benchmark();
        LOG.info("Created " + numOpsRequired + " files.");
      } else {
        LOG.info("useExisting = true. Assuming " 
            + numOpsRequired + " files have been created before.");
      }
      // use the same files for open
      super.generateInputs(opsPerThread);
      if(nameNodeProto.getFileInfo(opCreate.getBaseDir()) != null
          && nameNodeProto.getFileInfo(getBaseDir()) == null) {
        nameNodeProto.rename(opCreate.getBaseDir(), getBaseDir());
      }
      if(nameNodeProto.getFileInfo(getBaseDir()) == null) {
        throw new IOException(getBaseDir() + " does not exist.");
      }
    }

    /**
     * Do file open.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      nameNodeProto.getBlockLocations(fileNames[daemonId][inputIdx], 0L, BLOCK_SIZE);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * Delete file statistics.
   * 
   * Measure how many delete calls the name-node can handle per second.
   */
  class DeleteFileStats extends OpenFileStats {
    // Operation types
    static final String OP_DELETE_NAME = "delete";
    static final String OP_DELETE_USAGE = 
      "-op " + OP_DELETE_NAME + OP_USAGE_ARGS;

    DeleteFileStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_DELETE_NAME;
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      nameNodeProto.delete(fileNames[daemonId][inputIdx], false);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * List file status statistics.
   * 
   * Measure how many get-file-status calls the name-node can handle per second.
   */
  class FileStatusStats extends OpenFileStats {
    // Operation types
    static final String OP_FILE_STATUS_NAME = "fileStatus";
    static final String OP_FILE_STATUS_USAGE = 
      "-op " + OP_FILE_STATUS_NAME + OP_USAGE_ARGS;

    FileStatusStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_FILE_STATUS_NAME;
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      nameNodeProto.getFileInfo(fileNames[daemonId][inputIdx]);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * Rename file statistics.
   * 
   * Measure how many rename calls the name-node can handle per second.
   */
  class RenameFileStats extends OpenFileStats {
    // Operation types
    static final String OP_RENAME_NAME = "rename";
    static final String OP_RENAME_USAGE = 
      "-op " + OP_RENAME_NAME + OP_USAGE_ARGS;

    protected String[][] destNames;

    RenameFileStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_RENAME_NAME;
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      super.generateInputs(opsPerThread);
      destNames = new String[fileNames.length][];
      for(int idx=0; idx < numThreads; idx++) {
        int nrNames = fileNames[idx].length;
        destNames[idx] = new String[nrNames];
        for(int jdx=0; jdx < nrNames; jdx++)
          destNames[idx][jdx] = fileNames[idx][jdx] + ".r";
      }
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      nameNodeProto.rename(fileNames[daemonId][inputIdx],
                      destNames[daemonId][inputIdx]);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * Minimal data-node simulator.
   */
  private static class TinyDatanode implements Comparable<String> {
    private static final long DF_CAPACITY = 100*1024*1024;
    private static final long DF_USED = 0;
    
    NamespaceInfo nsInfo;
    DatanodeRegistration dnRegistration;
    DatanodeStorage storage; //only one storage 
    final ArrayList<BlockReportReplica> blocks;
    int nrBlocks; // actual number of blocks
    BlockListAsLongs blockReportList;
    final int dnIdx;
    int capacity = 10;

    private static int getNodePort(int num) throws IOException {
      int port = 1 + num;
      Preconditions.checkState(port < Short.MAX_VALUE);
      return port;
    }

    public static Comparator<TinyDatanode> TinyDatanodeComparator 
                          = new Comparator<TinyDatanode>() {

        public int compare(TinyDatanode dn1, TinyDatanode dn2) {
            
          String dnName1 = dn1.getXferAddr();
          String dnName2 = dn2.getXferAddr();
          
          //ascending order
          return dnName1.compareTo(dnName2);
          
          //descending order
          //return fruitName2.compareTo(fruitName1);
        }

    };

    TinyDatanode(int dnIdx, int blockCapacity) throws IOException {
      this.dnIdx = dnIdx;
      this.blocks = new ArrayList<BlockReportReplica>(blockCapacity);
      this.nrBlocks = 0;
      this.capacity = blockCapacity;
    }

    @Override
    public String toString() {
      return dnRegistration.toString();
    }

    String getXferAddr() {
      return dnRegistration.getXferAddr();
    }

    void register() throws IOException {
      // get versions from the namenode
      nsInfo = nameNodeProto.versionRequest();
      dnRegistration = new DatanodeRegistration(
          new DatanodeID(DNS.getDefaultIP("default"),
              DNS.getDefaultHost("default", "default"),
              DataNode.generateUuid(), getNodePort(dnIdx),
              DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
              DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
              DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT),
          new DataStorage(nsInfo),
          new ExportedBlockKeys(), VersionInfo.getVersion());
      // register datanode
      dnRegistration = nameNodeProto.registerDatanode(dnRegistration);
      //first block reports
      storage = new DatanodeStorage(DatanodeStorage.generateUuid());
      final StorageBlockReport[] reports = {
          new StorageBlockReport(storage, BlockListAsLongs.EMPTY)
      };
      nameNodeProto.blockReport(dnRegistration, 
          nameNode.getNamesystem().getBlockPoolId(), reports,
              new BlockReportContext(1, 0, System.nanoTime()));
    }

    /**
     * Send a heartbeat to the name-node.
     * Ignore reply commands.
     */
    void sendHeartbeat() throws IOException {
      // register datanode
      // TODO:FEDERATION currently a single block pool is supported
      StorageReport[] rep = { new StorageReport(storage, false,
          DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
      DatanodeCommand[] cmds = nameNodeProto.sendHeartbeat(dnRegistration, rep,
          0L, 0L, 0, 0, 0, null).getCommands();
      if(cmds != null) {
        for (DatanodeCommand cmd : cmds ) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("sendHeartbeat Name-node reply: " + cmd.getAction());
          }
        }
      }
    }

    boolean addBlock(Block blk) {
      if(nrBlocks >= this.capacity) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Cannot add block: datanode capacity = " + blocks.size());
        }
        return false;
      }
      blocks.add(new BlockReportReplica(blk));
      nrBlocks++;
      return true;
    }

    void formBlockReport() {
      // fill remaining slots with blocks that do not exist
      for (int idx = blocks.size()-1; idx >= nrBlocks; idx--) {
        Block block = new Block(blocks.size() - idx, 0, 0);
        blocks.set(idx, new BlockReportReplica(block));
      }
      blockReportList = BlockListAsLongs.EMPTY;
    }

    BlockListAsLongs getBlockReportList() {
      return blockReportList;
    }

    @Override
    public int compareTo(String xferAddr) {
      return getXferAddr().compareTo(xferAddr);
    }

    /**
     * Send a heartbeat to the name-node and replicate blocks if requested.
     */
    @SuppressWarnings("unused") // keep it for future blockReceived benchmark
    int replicateBlocks() throws IOException {
      // register datanode
      StorageReport[] rep = { new StorageReport(storage,
          false, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
      DatanodeCommand[] cmds = nameNodeProto.sendHeartbeat(dnRegistration,
          rep, 0L, 0L, 0, 0, 0, null).getCommands();
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          if (cmd.getAction() == DatanodeProtocol.DNA_TRANSFER) {
            // Send a copy of a block to another datanode
            BlockCommand bcmd = (BlockCommand)cmd;
            return transferBlocks(bcmd.getBlocks(), bcmd.getTargets(),
                                  bcmd.getTargetStorageIDs());
          }
        }
      }
      return 0;
    }

    /**
     * Transfer blocks to another data-node.
     * Just report on behalf of the other data-node
     * that the blocks have been received.
     */
    private int transferBlocks( Block blocks[], 
                                DatanodeInfo xferTargets[][],
                                String targetStorageIDs[][]
                              ) throws IOException {
      for(int i = 0; i < blocks.length; i++) {
        DatanodeInfo blockTargets[] = xferTargets[i];
        for(int t = 0; t < blockTargets.length; t++) {
          DatanodeInfo dnInfo = blockTargets[t];
          String targetStorageID = targetStorageIDs[i][t];
          DatanodeRegistration receivedDNReg;
          receivedDNReg = new DatanodeRegistration(dnInfo,
            new DataStorage(nsInfo),
            new ExportedBlockKeys(), VersionInfo.getVersion());
          ReceivedDeletedBlockInfo[] rdBlocks = {
            new ReceivedDeletedBlockInfo(
                  blocks[i], ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
                  null) };
          StorageReceivedDeletedBlocks[] report = { new StorageReceivedDeletedBlocks(
              targetStorageID, rdBlocks) };
          nameNodeProto.blockReceivedAndDeleted(receivedDNReg, nameNode
              .getNamesystem().getBlockPoolId(), report);
        }
      }
      return blocks.length;
    }
  }

  /**
   * Block report statistics.
   * 
   * Each thread here represents its own data-node.
   * Data-nodes send the same block report each time.
   * The block report may contain missing or non-existing blocks.
   */
  class BlockReportStats extends OperationStatsBase {
    static final String OP_BLOCK_REPORT_NAME = "blockReport";
    static final String OP_BLOCK_REPORT_USAGE = 
      "-op blockReport [-datanodes T] [-reports N] " +
      "[-blocksPerReport B] [-blocksPerFile F] [-writerPoolSize P]";

    private int blocksPerReport;
    private int blocksPerFile;
    private TinyDatanode[] datanodes; // array of data-nodes sorted by name
    private long nrBlocks;
    private long nrFiles;
    private int writerPoolSize;
    private SimpleStat createStat = new SimpleStat("create");
    private long nnStart = 0, nnEnd = 0;
    private int waiting = 0;
    private int longestWait = 0;
    private Object mutex = new Object();
    
    // for GEDA evaluation
    private int longestQueueSize = 0;
    BlockingQueue<Runnable> consumerQueue = new LinkedBlockingQueue<Runnable>();
    ExecutorService producer = null;
    ExecutorService consumer = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, consumerQueue);
    
    BlockReportStats(List<String> args) {
      super();
      this.blocksPerReport = 100;
      this.blocksPerFile = 10;
      this.writerPoolSize = 8;
      // set heartbeat interval to 3 min, so that expiration were 40 min
      config.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3 * 60);
      parseArguments(args);
      // adjust replication to the number of data-nodes
      this.replication = (short)Math.min(replication, getNumDatanodes());
    }
    
    class FileWriteTask implements Runnable {
      private String fileName;
      private String clientName;

      public FileWriteTask(String name, String clientName) {
        this.fileName = name;
        this.clientName = clientName;
      }

      public String getName() {
        return fileName;
      }

      @Override
      public void run() {
        try {
          long start, end, time;
          start = Time.monotonicNow();
          blockingNameNodeCreate(fileName, FsPermission.getDefault(), clientName,
              new EnumSetWritable<CreateFlag>(
                  EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)),
              true, replication, BLOCK_SIZE, null);
          end = Time.monotonicNow();
          time = end-start;
          createStat.addValue(time);
          
    	  ExtendedBlock lastBlock = addBlocks(fileName, clientName);
    	  
          blockingNameNodeComplete(fileName, clientName, lastBlock,
              INodeId.GRANDFATHER_INODE_ID);
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }
    
    // for GEDA evaluation
    class GedaFileTask implements Runnable {
    	private int taskStage;
    	private String filename;
    	private String clientname;
    	private ExtendedBlock prevBlock;
    	private LocatedBlock loc;
    	private int jdx;
    	private int infoidx;
    	private DatanodeRegistration registration;
    	private StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks;
    	private long startCreate;
    	private long endCreate;
    	
    	public GedaFileTask(String filename, String clientname){
    		this.taskStage = 0;
    		this.filename = filename;
    		this.clientname = clientname;
    		this.prevBlock = null;
    		this.loc = null;
    		this.jdx = 0;
    		this.infoidx = 0;
    		this.registration = null;
    		this.rcvdAndDeletedBlocks = null;
    		this.startCreate = 0;
    		this.endCreate = 0;
    	}
    	
    	public void checkLongestQueueSize(){
    		LOG.info("longestQueueSize=" + longestQueueSize + " consumerQueueSize=" + consumerQueue.size() + " process=" + filename);
    		if(longestQueueSize < consumerQueue.size()){
    			longestQueueSize = consumerQueue.size();
    		}
    	}
    	
    	@Override
    	public void run() {
    		try {
	    		if(taskStage == 0){
		            taskStage++;
		            this.startCreate = Time.monotonicNow();
		            checkLongestQueueSize();
		            consumer.execute(this);
	    		} else if(taskStage == 1){
	    			taskStage++;
	    			nameNodeProto.create(filename, FsPermission.getDefault(), clientname, 
	    					new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)),
	    		                  true, replication, BLOCK_SIZE, null);
		            this.endCreate = Time.monotonicNow();
		            createStat.addValue(endCreate - startCreate);
		            checkLongestQueueSize();
	    			consumer.execute(this);
	    		} else if(taskStage == 2){
	    			taskStage++;
	    			this.loc = nameNodeProto.addBlock(filename, clientname, prevBlock, null, INodeId.GRANDFATHER_INODE_ID, null);
	    			producer.execute(this);
	    		} else if(taskStage == 3){
	    			taskStage++;
	    			prevBlock = loc.getBlock();
	    			for(int infoidx = 0; infoidx < loc.getLocations().length; infoidx++){
	    				this.infoidx = infoidx;
		    			DatanodeInfo dnInfo = loc.getLocations()[infoidx];
		  		        int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());
		  		        datanodes[dnIdx].addBlock(loc.getBlock().getLocalBlock());
		  		        ReceivedDeletedBlockInfo[] rdBlocks = { new ReceivedDeletedBlockInfo(
		  		            loc.getBlock().getLocalBlock(),
		  		            ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null) };
		  		        StorageReceivedDeletedBlocks[] report = { new StorageReceivedDeletedBlocks(
		  		            datanodes[dnIdx].storage.getStorageID(), rdBlocks) };
		  		        rcvdAndDeletedBlocks = report;
		  		        registration = datanodes[dnIdx].dnRegistration;
			            checkLongestQueueSize();
		  		        consumer.execute(this);
	    			}
	    		} else if(taskStage == 4){
	    			taskStage++;
	    			nameNodeProto.blockReceivedAndDeleted(registration, loc.getBlock().getBlockPoolId(),
	    		            rcvdAndDeletedBlocks);
	    			
	    			if(this.infoidx >= loc.getLocations().length){
	    				jdx++;
		    			if(jdx < blocksPerFile){
		    				taskStage = 2;
				            checkLongestQueueSize();
		    				consumer.execute(this);
		    			} else {
		    				taskStage++;
				            checkLongestQueueSize();
		    				consumer.execute(this);
		    			}
	    			}
	    		} else if(taskStage == 5){
	    			nameNodeProto.complete(filename, clientname, prevBlock,
	    		              INodeId.GRANDFATHER_INODE_ID);
	    		}
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    }
    
    class FileReportTask implements Runnable {
    	private String fileName;
        private String clientName;
        private LocatedBlock[] writtenBlocksLocation;

        public FileReportTask(String name, String clientName, LocatedBlock[] writtenBlocksLocation) {
          this.fileName = name;
          this.clientName = clientName;
          this.writtenBlocksLocation = writtenBlocksLocation;
        }

        public String getName() {
          return fileName;
        }
        
        @Override
        public void run() {
          try {
        	ExtendedBlock lastBlock = receivedAndDeletedBlocks(writtenBlocksLocation);

            blockingNameNodeComplete(fileName, clientName, lastBlock,
                INodeId.GRANDFATHER_INODE_ID);
          } catch (IOException ex) {
            ex.printStackTrace();
          }
        }
    }

    /**
     * Each thread pretends its a data-node here.
     */
    private int getNumDatanodes() {
      return numThreads;
    }

    @Override
    String getOpName() {
      return OP_BLOCK_REPORT_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-reports")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-datanodes")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-blocksPerReport")) {
          if(i+1 == args.size())  printUsage();
          blocksPerReport = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-blocksPerFile")) {
          if(i+1 == args.size())  printUsage();
          blocksPerFile = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-writerPoolSize")) {
          if(i+1 == args.size())  printUsage();
          writerPoolSize = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-isGEDA")){
          isGEDAmode = true;
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
    }

    private HdfsFileStatus blockingNameNodeCreate(String src,
        FsPermission masked, String clientName,
        EnumSetWritable<CreateFlag> flag, boolean createParent,
        short replication, long blockSize,
        CryptoProtocolVersion[] supportedVersions)
        throws SnapshotAccessControlException, AccessControlException,
        DSQuotaExceededException, NSQuotaExceededException,
        AlreadyBeingCreatedException, FileAlreadyExistsException,
        FileNotFoundException, ParentNotDirectoryException, SafeModeException,
        UnresolvedLinkException, IOException {
      waiting++;
      synchronized (mutex) {
        if (longestWait < waiting)
          longestWait = waiting;
        waiting--;
        return nameNodeProto.create(src, masked, clientName, flag, createParent,
            replication, blockSize, supportedVersions);
      }
    }

    private boolean blockingNameNodeComplete(String src, String clientName,
        ExtendedBlock last, long fileId)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {
      waiting++;
      synchronized (mutex) {
        if (longestWait < waiting)
          longestWait = waiting;
        waiting--;
        return nameNodeProto.complete(src, clientName, last, fileId);
      }
    }

    private LocatedBlock blockingNameNodeAddBlock(String src, String clientName,
        ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
        String[] favoredNodes) throws AccessControlException,
        FileNotFoundException, NotReplicatedYetException, SafeModeException,
        UnresolvedLinkException, IOException {
      waiting++;
      synchronized (mutex) {
        if (longestWait < waiting)
          longestWait = waiting;
        waiting--;
        return nameNodeProto.addBlock(src, clientName, previous, excludeNodes,
            fileId, favoredNodes);
      }
    }

    public void blockingNameNodeBlockReceivedAndDeleted(
        DatanodeRegistration registration, String poolId,
        StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
        throws IOException {
      waiting++;
      synchronized (mutex) {
        if (longestWait < waiting)
          longestWait = waiting;
        waiting--;
        nameNodeProto.blockReceivedAndDeleted(registration, poolId,
            rcvdAndDeletedBlocks);
      }
    }

    @Override
    void generateInputs(int[] ignore) throws IOException {
      int nrDatanodes = getNumDatanodes();
      nrBlocks = (int)Math.ceil((double)blocksPerReport * nrDatanodes 
                                    / replication);
      nrFiles = (int)Math.ceil((double)nrBlocks / blocksPerFile);
      //nrFiles = 1000;
      datanodes = new TinyDatanode[nrDatanodes];
      // create data-nodes
      String prevDNName = "";
      for(int idx=0; idx < nrDatanodes; idx++) {
        datanodes[idx] = new TinyDatanode(idx, (int)(blocksPerReport*1.5));
        datanodes[idx].register();
        assert datanodes[idx].getXferAddr().compareTo(prevDNName) > 0
          : "Data-nodes must be sorted lexicographically.";
        datanodes[idx].sendHeartbeat();
        prevDNName = datanodes[idx].getXferAddr();
      }
      Arrays.sort(datanodes, TinyDatanode.TinyDatanodeComparator);

      try {
        // create files
        LOG.info("Creating " + nrFiles + " files with " + blocksPerFile + " blocks each.");
        FileNameGenerator nameGenerator;
        nameGenerator = new FileNameGenerator(getBaseDir(), 100);
        String clientName = getClientName(007);
        nameNodeProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
            false);
        if(isGEDAmode){
        	producer = Executors.newFixedThreadPool(12);
            nnStart = Time.monotonicNow();
	        for(int idx=0; idx < nrFiles; idx++) {
	          String fileName = nameGenerator.getNextFileName("ThroughputBench");
	          
	          GedaFileTask task = new GedaFileTask(fileName, clientName);
	          producer.execute(task);
	          LOG.info("Loaded file=" + fileName + " to Producer.");
	        }
	        
	        while(consumerQueue.size() > 0){
	        	Thread.sleep(500);
	        }
	        
	        producer.shutdown();
	        consumer.shutdown();
        } else {
	        ExecutorService executor = Executors.newFixedThreadPool(writerPoolSize);
	        nnStart = Time.monotonicNow();
	        for(int idx=0; idx < nrFiles; idx++) {
	          String fileName = nameGenerator.getNextFileName("ThroughputBench");
	          //nameNodeProto.create(fileName, FsPermission.getDefault(), clientName,
	          //    new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, replication,
	          //    BLOCK_SIZE, null);
	          //ExtendedBlock lastBlock = addBlocks(fileName, clientName);
	          //nameNodeProto.complete(fileName, clientName, lastBlock, INodeId.GRANDFATHER_INODE_ID);
	          FileWriteTask task = new FileWriteTask(fileName, clientName);
	          executor.execute(task);
	          //if (idx % 1000 == 0) {
	          //  printStatAndSleep(false);
	          //}
	        }
	
	        executor.shutdown();
	        executor.awaitTermination(1, TimeUnit.HOURS);
        }
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      } finally {
        nnEnd = Time.monotonicNow();
        printStatAndSleep(true);
      }
      // prepare block reports
      for(int idx=0; idx < nrDatanodes; idx++) {
        datanodes[idx].formBlockReport();
      }
    }

    private void printStatAndSleep(boolean appendCDF) {
      SimpleStat ibrStat = nameNodeProto.getIncrementalBlockReportStat();
      SimpleStat nnCreateStat = nameNodeProto.getCreateStat();
      SimpleStat writeLockStat = nameNodeProto.getWriteLockStat();
      long nnLifetime = nnEnd - nnStart;

      LOG.info("--- experiment stats ---");
      LOG.info("nrFiles  = " + nrFiles);
      LOG.info("nrBlocks = " + nrBlocks);
      LOG.info("poolSize = " + writerPoolSize);
      LOG.info("--- create stats (ms) ---");
      LOG.info("creat min = " + createStat.getMin());
      LOG.info("creat max = " + createStat.getMax());
      LOG.info("creat avg = " + createStat.getAvg());
      LOG.info("createct  = " + createStat.getCount());
      LOG.info("creat sum = " + createStat.getSum());
      LOG.info("--- NN create stats (ns) ---");
      LOG.info("nnCrt min = " + nnCreateStat.getMin());
      LOG.info("nnCrt max = " + nnCreateStat.getMax());
      LOG.info("nnCrt avg = " + nnCreateStat.getAvg());
      LOG.info("nnCrt ct  = " + nnCreateStat.getCount());
      LOG.info("nnCrt sum = " + nnCreateStat.getSum());
      LOG.info("--- writeLock stats (ns) ---");
      LOG.info("wLock min = " + writeLockStat.getMin());
      LOG.info("wLock max = " + writeLockStat.getMax());
      LOG.info("wLock avg = " + writeLockStat.getAvg());
      LOG.info("wLock ct  = " + writeLockStat.getCount());
      LOG.info("wLock sum = " + writeLockStat.getSum());
      LOG.info("--- IBR stats (ns) ---");
      LOG.info("ibrSt min = " + ibrStat.getMin());
      LOG.info("ibrSt max = " + ibrStat.getMax());
      LOG.info("ibrSt avg = " + ibrStat.getAvg());
      LOG.info("ibrSt ct  = " + ibrStat.getCount());
      LOG.info("ibrSt sum = " + ibrStat.getSum());
      LOG.info("--- NN Lifetime ---");
      LOG.info("lifetime (ms)    = " + nnLifetime);
      LOG.info("%life in IBR     = " + ((double)ibrStat.getSum()/1000000.0/nnLifetime));
      LOG.info("IBR/ms           = " + ((double)ibrStat.getCount()/nnLifetime));
      LOG.info("%life in create  = " + ((double)nnCreateStat.getSum()/1000000.0/nnLifetime));
      LOG.info("%life in lock    = " + ((double)writeLockStat.getSum()/1000000.0/nnLifetime));
      LOG.info("longestNNRpcWait = " + longestWait);
      LOG.info("longestNNGEDAQueueSize = " + longestQueueSize);

      try (BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/stat.out", true))) {
        bw.write("--- create stats ---\n");
        bw.write(createStat.toString());
        LOG.info("riza: delay write for 1000 ms ");
        Thread.sleep(1);
      } catch (IOException e) { 
        e.printStackTrace();
      } catch(InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (appendCDF) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/create-lat.dat", false))) {
          bw.write(createStat.getCDFDataString());
        } catch (IOException e) { 
          e.printStackTrace();
        }
      }
    }

    private ExtendedBlock addBlocks(String fileName, String clientName)
    throws IOException {
      ExtendedBlock prevBlock = null;
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = blockingNameNodeAddBlock(fileName, clientName,
            prevBlock, null, INodeId.GRANDFATHER_INODE_ID, null);
        prevBlock = loc.getBlock();
        for(DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());
          datanodes[dnIdx].addBlock(loc.getBlock().getLocalBlock());
          ReceivedDeletedBlockInfo[] rdBlocks = { new ReceivedDeletedBlockInfo(
              loc.getBlock().getLocalBlock(),
              ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null) };
          StorageReceivedDeletedBlocks[] report = { new StorageReceivedDeletedBlocks(
              datanodes[dnIdx].storage.getStorageID(), rdBlocks) };
          blockingNameNodeBlockReceivedAndDeleted(datanodes[dnIdx].dnRegistration, loc
              .getBlock().getBlockPoolId(), report);
        }
      }
      return prevBlock;
    }
    
    private LocatedBlock[] addBlocksWithoutReport(String fileName, String clientName)
    throws IOException {
      LocatedBlock[] prevLocatedBlock = new LocatedBlock[blocksPerFile];
      ExtendedBlock prevBlock = null;
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = blockingNameNodeAddBlock(fileName, clientName,
            prevBlock, null, INodeId.GRANDFATHER_INODE_ID, null);
        prevLocatedBlock[jdx] = loc;
        prevBlock = loc.getBlock();
        for(DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());
          datanodes[dnIdx].addBlock(loc.getBlock().getLocalBlock());
        }
      }
      return prevLocatedBlock;
    }
    
    private ExtendedBlock receivedAndDeletedBlocks(LocatedBlock[] loc) throws IOException{
    	ExtendedBlock prevBlock = loc[loc.length-1].getBlock();;
    	for(int jdx = 0; jdx < loc.length; jdx++) {
			for(DatanodeInfo dnInfo : loc[jdx].getLocations()) {
		        int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());
		        ReceivedDeletedBlockInfo[] rdBlocks = { new ReceivedDeletedBlockInfo(
		            loc[jdx].getBlock().getLocalBlock(), ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null) };
		        StorageReceivedDeletedBlocks[] report = { new StorageReceivedDeletedBlocks(
		            datanodes[dnIdx].storage.getStorageID(), rdBlocks) };
		        blockingNameNodeBlockReceivedAndDeleted(datanodes[dnIdx].dnRegistration, loc[jdx].getBlock().getBlockPoolId(), report);
		    }
    	}
    	return prevBlock;
    }

    /**
     * Does not require the argument
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return null;
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
      assert daemonId < numThreads : "Wrong daemonId.";
      TinyDatanode dn = datanodes[daemonId];
      long start = Time.now();
      StorageBlockReport[] report = { new StorageBlockReport(
          dn.storage, dn.getBlockReportList()) };
      nameNodeProto.blockReport(dn.dnRegistration,
          nameNode.getNamesystem().getBlockPoolId(), report,
          new BlockReportContext(1, 0, System.nanoTime()));
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      String blockDistribution = "";
      String delim = "(";
      for(int idx=0; idx < getNumDatanodes(); idx++) {
        blockDistribution += delim + datanodes[idx].nrBlocks;
        delim = ", ";
      }
      blockDistribution += ")";
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("reports = " + numOpsRequired);
      LOG.info("datanodes = " + numThreads + " " + blockDistribution);
      LOG.info("blocksPerReport = " + blocksPerReport);
      LOG.info("blocksPerFile = " + blocksPerFile);
      printStats();
    }
  }   // end BlockReportStats

  /**
   * Measures how fast replication monitor can compute data-node work.
   * 
   * It runs only one thread until no more work can be scheduled.
   */
  class ReplicationStats extends OperationStatsBase {
    static final String OP_REPLICATION_NAME = "replication";
    static final String OP_REPLICATION_USAGE = 
      "-op replication [-datanodes T] [-nodesToDecommission D] " +
      "[-nodeReplicationLimit C] [-totalBlocks B] [-replication R]";

    private final BlockReportStats blockReportObject;
    private int numDatanodes;
    private int nodesToDecommission;
    private int nodeReplicationLimit;
    private int totalBlocks;
    private int numDecommissionedBlocks;
    private int numPendingBlocks;

    ReplicationStats(List<String> args) {
      super();
      numThreads = 1;
      numDatanodes = 3;
      nodesToDecommission = 1;
      nodeReplicationLimit = 100;
      totalBlocks = 100;
      parseArguments(args);
      // number of operations is 4 times the number of decommissioned
      // blocks divided by the number of needed replications scanned 
      // by the replication monitor in one iteration
      numOpsRequired = (totalBlocks*replication*nodesToDecommission*2)
            / (numDatanodes*numDatanodes);

      String[] blkReportArgs = {
        "-op", "blockReport",
        "-datanodes", String.valueOf(numDatanodes),
        "-blocksPerReport", String.valueOf(totalBlocks*replication/numDatanodes),
        "-blocksPerFile", String.valueOf(numDatanodes)};
      blockReportObject = new BlockReportStats(Arrays.asList(blkReportArgs));
      numDecommissionedBlocks = 0;
      numPendingBlocks = 0;
    }

    @Override
    String getOpName() {
      return OP_REPLICATION_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-datanodes")) {
          if(i+1 == args.size())  printUsage();
          numDatanodes = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-nodesToDecommission")) {
          if(i+1 == args.size())  printUsage();
          nodesToDecommission = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-nodeReplicationLimit")) {
          if(i+1 == args.size())  printUsage();
          nodeReplicationLimit = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-totalBlocks")) {
          if(i+1 == args.size())  printUsage();
          totalBlocks = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-replication")) {
          if(i+1 == args.size())  printUsage();
          replication = Short.parseShort(args.get(++i));
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
    }

    @Override
    void generateInputs(int[] ignore) throws IOException {
      final FSNamesystem namesystem = nameNode.getNamesystem();

      // start data-nodes; create a bunch of files; generate block reports.
      blockReportObject.generateInputs(ignore);
      // stop replication monitor
      BlockManagerTestUtil.stopReplicationThread(namesystem.getBlockManager());

      // report blocks once
      int nrDatanodes = blockReportObject.getNumDatanodes();
      for(int idx=0; idx < nrDatanodes; idx++) {
        blockReportObject.executeOp(idx, 0, null);
      }
      // decommission data-nodes
      decommissionNodes();
      // set node replication limit
      BlockManagerTestUtil.setNodeReplicationLimit(namesystem.getBlockManager(),
          nodeReplicationLimit);
    }

    private void decommissionNodes() throws IOException {
      String excludeFN = config.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, "exclude");
      FileOutputStream excludeFile = new FileOutputStream(excludeFN);
      excludeFile.getChannel().truncate(0L);
      int nrDatanodes = blockReportObject.getNumDatanodes();
      numDecommissionedBlocks = 0;
      for(int i=0; i < nodesToDecommission; i++) {
        TinyDatanode dn = blockReportObject.datanodes[nrDatanodes-1-i];
        numDecommissionedBlocks += dn.nrBlocks;
        excludeFile.write(dn.getXferAddr().getBytes());
        excludeFile.write('\n');
        LOG.info("Datanode " + dn + " is decommissioned.");
      }
      excludeFile.close();
      nameNodeProto.refreshNodes();
    }

    /**
     * Does not require the argument
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return null;
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
      assert daemonId < numThreads : "Wrong daemonId.";
      long start = Time.now();
      // compute data-node work
      int work = BlockManagerTestUtil.getComputedDatanodeWork(
          nameNode.getNamesystem().getBlockManager());
      long end = Time.now();
      numPendingBlocks += work;
      if(work == 0)
        daemons.get(daemonId).terminate();
      return end-start;
    }

    @Override
    void printResults() {
      String blockDistribution = "";
      String delim = "(";
      for(int idx=0; idx < blockReportObject.getNumDatanodes(); idx++) {
        blockDistribution += delim + blockReportObject.datanodes[idx].nrBlocks;
        delim = ", ";
      }
      blockDistribution += ")";
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("numOpsRequired = " + numOpsRequired);
      LOG.info("datanodes = " + numDatanodes + " " + blockDistribution);
      LOG.info("decommissioned datanodes = " + nodesToDecommission);
      LOG.info("datanode replication limit = " + nodeReplicationLimit);
      LOG.info("total blocks = " + totalBlocks);
      printStats();
      LOG.info("decommissioned blocks = " + numDecommissionedBlocks);
      LOG.info("pending replications = " + numPendingBlocks);
      LOG.info("replications per sec: " + getBlocksPerSecond());
    }

    private double getBlocksPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numPendingBlocks / elapsedTime;
    }

  }   // end ReplicationStats

  static void printUsage() {
    System.err.println("Usage: NNThroughputBenchmark"
        + "\n\t"    + OperationStatsBase.OP_ALL_USAGE
        + " | \n\t" + CreateFileStats.OP_CREATE_USAGE
        + " | \n\t" + MkdirsStats.OP_MKDIRS_USAGE
        + " | \n\t" + OpenFileStats.OP_OPEN_USAGE
        + " | \n\t" + DeleteFileStats.OP_DELETE_USAGE
        + " | \n\t" + FileStatusStats.OP_FILE_STATUS_USAGE
        + " | \n\t" + RenameFileStats.OP_RENAME_USAGE
        + " | \n\t" + BlockReportStats.OP_BLOCK_REPORT_USAGE
        + " | \n\t" + ReplicationStats.OP_REPLICATION_USAGE
        + " | \n\t" + CleanAllStats.OP_CLEAN_USAGE
        + " | \n\t" + GENERAL_OPTIONS_USAGE
    );
    System.exit(-1);
  }

  public static void runBenchmark(Configuration conf, List<String> args)
      throws Exception {
    NNThroughputBenchmark bench = null;
    try {
      bench = new NNThroughputBenchmark(conf);
      bench.run(args.toArray(new String[]{}));
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  /**
   * Main method of the benchmark.
   * @param aArgs command line parameters
   */
  @Override // Tool
  public int run(String[] aArgs) throws Exception {
    List<String> args = new ArrayList<String>(Arrays.asList(aArgs));
    if(args.size() < 2 || ! args.get(0).startsWith("-op"))
      printUsage();

    String type = args.get(1);
    boolean runAll = OperationStatsBase.OP_ALL_NAME.equals(type);

    // Start the NameNode
    String[] argv = new String[] {};
    nameNode = NameNode.createNameNode(argv, config);
    nameNodeProto = nameNode.getRpcServer();

    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
    try {
      if(runAll || CreateFileStats.OP_CREATE_NAME.equals(type)) {
        opStat = new CreateFileStats(args);
        ops.add(opStat);
      }
      if(runAll || MkdirsStats.OP_MKDIRS_NAME.equals(type)) {
        opStat = new MkdirsStats(args);
        ops.add(opStat);
      }
      if(runAll || OpenFileStats.OP_OPEN_NAME.equals(type)) {
        opStat = new OpenFileStats(args);
        ops.add(opStat);
      }
      if(runAll || DeleteFileStats.OP_DELETE_NAME.equals(type)) {
        opStat = new DeleteFileStats(args);
        ops.add(opStat);
      }
      if(runAll || FileStatusStats.OP_FILE_STATUS_NAME.equals(type)) {
        opStat = new FileStatusStats(args);
        ops.add(opStat);
      }
      if(runAll || RenameFileStats.OP_RENAME_NAME.equals(type)) {
        opStat = new RenameFileStats(args);
        ops.add(opStat);
      }
      if(runAll || BlockReportStats.OP_BLOCK_REPORT_NAME.equals(type)) {
        opStat = new BlockReportStats(args);
        ops.add(opStat);
      }
      if(runAll || ReplicationStats.OP_REPLICATION_NAME.equals(type)) {
        opStat = new ReplicationStats(args);
        ops.add(opStat);
      }
      if(runAll || CleanAllStats.OP_CLEAN_NAME.equals(type)) {
        opStat = new CleanAllStats(args);
        ops.add(opStat);
      }
      if(ops.size() == 0)
        printUsage();
      // run each benchmark
      for(OperationStatsBase op : ops) {
        LOG.info("Starting benchmark: " + op.getOpName());
        op.benchmark();
        op.cleanUp();
      }
      // print statistics
      for(OperationStatsBase op : ops) {
        LOG.info("");
        op.printResults();
      }
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    NNThroughputBenchmark bench = null;
    try {
      bench = new NNThroughputBenchmark(new HdfsConfiguration());
      ToolRunner.run(bench, args);
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override // Configurable
  public Configuration getConf() {
    return config;
  }
}
