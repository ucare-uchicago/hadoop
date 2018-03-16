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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@SuppressWarnings({"unchecked", "rawtypes"})
public class Shuffle<K, V> implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {
  private static final Log LOG = LogFactory.getLog(Shuffle.class);	
  private static final int PROGRESS_FREQUENCY = 2000;
  private static final int MAX_EVENTS_TO_FETCH = 10000;
  private static final int MIN_EVENTS_TO_FETCH = 100;
  private static final int MAX_RPC_OUTSTANDING_EVENTS = 3000000;
  
  private ShuffleConsumerPlugin.Context context;

  private TaskAttemptID reduceId;
  private JobConf jobConf;
  private Reporter reporter;
  private ShuffleClientMetrics metrics;
  private TaskUmbilicalProtocol umbilical;
  
  private ShuffleSchedulerImpl<K,V> scheduler;
  private MergeManager<K, V> merger;
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private Progress copyPhase;
  private TaskStatus taskStatus;
  private Task reduceTask; //Used for status updates
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  // @Cesar: Some params
  private boolean isFetchRateSpeculationEnabled = false;
  private boolean isFetcherShutDownEnabled = false;
  
  @Override
  public void init(ShuffleConsumerPlugin.Context context) {
    this.context = context;
    this.reduceId = context.getReduceId();
    this.jobConf = context.getJobConf();
    this.umbilical = context.getUmbilical();
    this.reporter = context.getReporter();
    this.metrics = new ShuffleClientMetrics(reduceId, jobConf);
    this.copyPhase = context.getCopyPhase();
    this.taskStatus = context.getStatus();
    this.reduceTask = context.getReduceTask();
    this.localMapFiles = context.getLocalMapFiles();
    
    scheduler = new ShuffleSchedulerImpl<K, V>(jobConf, taskStatus, reduceId,
        this, copyPhase, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());
    merger = createMergeManager(context);
    // @Cesar: Assign values in order to kill fetchers if possible
    isFetchRateSpeculationEnabled =
        jobConf.getBoolean(MRJobConfig.EXP_ENABLE_FETCH_RATE_SPECULATION,
            MRJobConfig.DEFAULT_EXP_ENABLE_FETCH_RATE_SPECULATION);
    isFetcherShutDownEnabled =
        jobConf.getBoolean(MRJobConfig.EXP_ENABLE_FETCHER_SHUTDOWN,
            MRJobConfig.DEFAULT_EXP_ENABLE_FETCHER_SHUTDOWN);
  }

  protected MergeManager<K, V> createMergeManager(
      ShuffleConsumerPlugin.Context context) {
    return new MergeManagerImpl<K, V>(reduceId, jobConf, context.getLocalFS(),
        context.getLocalDirAllocator(), reporter, context.getCodec(),
        context.getCombinerClass(), context.getCombineCollector(), 
        context.getSpilledRecordsCounter(),
        context.getReduceCombineInputCounter(),
        context.getMergedMapOutputsCounter(), this, context.getMergePhase(),
        context.getMapOutputFile());
  }

  @Override
  public RawKeyValueIterator run() throws IOException, InterruptedException {
    // Scale the maximum events we fetch per RPC call to mitigate OOM issues
    // on the ApplicationMaster when a thundering herd of reducers fetch events
    // TODO: This should not be necessary after HADOOP-8942
    int eventsPerReducer = Math.max(MIN_EVENTS_TO_FETCH,
        MAX_RPC_OUTSTANDING_EVENTS / jobConf.getNumReduceTasks());
    int maxEventsToFetch = Math.min(MAX_EVENTS_TO_FETCH, eventsPerReducer);

    // Start the map-completion events fetcher thread
    final EventFetcher<K,V> eventFetcher = 
      new EventFetcher<K,V>(reduceId, umbilical, scheduler, this,
          maxEventsToFetch);
    eventFetcher.start();
    
    // Start the map-output fetcher threads
    boolean isLocal = localMapFiles != null;
    final int numFetchers = isLocal ? 1 :
      jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
    Fetcher<K,V>[] fetchers = new Fetcher[numFetchers];
    if (isLocal) {
      fetchers[0] = new LocalFetcher<K, V>(jobConf, reduceId, scheduler,
          merger, reporter, metrics, this, reduceTask.getShuffleSecret(),
          localMapFiles);
      fetchers[0].start();
    } else {
      for (int i=0; i < numFetchers; ++i) {
        fetchers[i] = new Fetcher<K,V>(jobConf, reduceId, scheduler, merger, 
                                       reporter, metrics, this, 
                                       reduceTask.getShuffleSecret());
        fetchers[i].start();
      }
    }
    
    // @Cesar: Start measuring suffle time
    long shuffleTimeStart = System.nanoTime();
    
    LOG.info("@Cesar: Fetch reate speculation is enabled?: " + isFetchRateSpeculationEnabled);
    LOG.info("@Cesar: Fetcher shutdown is enabled?: " + isFetcherShutDownEnabled);
    // Wait for shuffle to complete successfully
    while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
      LOG.info("@Kris: remaining Maps: " + scheduler.getRemainingMaps());
      reporter.progress();
      
      synchronized (this) {
        if (throwable != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable);
        }
      }
      // @Cesar: First check params
      if(!(this.isFetcherShutDownEnabled && this.isFetchRateSpeculationEnabled)) continue;
      // @Cesar: Check the obsolete map outputs
      Set<TaskAttemptID> obsoletes = scheduler.getObsoleteMaps();
      for(int fetcherId = 0; fetcherId < fetchers.length; ++fetcherId){
    	  for(TaskAttemptID obsolete : obsoletes){
    		  LOG.info("@Cesar: Checking obsolete map output  " + obsolete
    				  + " on fetcher #" + (fetcherId + 1));
	    	  // @Cesar: Is map output marked as obsolete?
	    	  if(fetchers[fetcherId] != null && fetchers[fetcherId].isAlive() 
	    	     && fetchers[fetcherId].getFetcherAssignedMaps().contains(obsolete)){
	    		  // @Cesar: Yes? Well, then we interrupt this fetcher thread
	    		  LOG.info("@Cesar: Map output from " + obsolete + " declared as OBSOLETE, interrupting " + 
	    				   "fetcher #" + (fetcherId + 1));
	    		  fetchers[fetcherId].shutDown();
	    		  // @Cesar: Replace by new fetcher
	    		  fetchers[fetcherId] = new Fetcher<K,V>(jobConf, reduceId, scheduler, merger, 
								                         reporter, metrics, this, 
								                         reduceTask.getShuffleSecret(), fetcherId + 1, true);
	    		  LOG.info("@Cesar: New fetcher thread created with id " + (fetcherId + 1));
	    	  }
    	  }
    	  
      }
      
    }

    // Stop the event-fetcher thread
    eventFetcher.shutDown();
    
    // Stop the map-output fetcher threads
    for (Fetcher<K,V> fetcher : fetchers) {
      fetcher.shutDown();
    }
    
    // stop the scheduler
    scheduler.close();

    // @Cesar: Log shuffle finish time
    long shuffleTimeStop = System.nanoTime();
    LOG.info(UcareSeShuffleMessage.createUcareSeMessageShuffleFinished(shuffleTimeStop - shuffleTimeStart));
    
    copyPhase.complete(); // copy is already complete
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);
    
    // Finish the on-going merges...
    RawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge " , e);
    }

    // Sanity check
    synchronized (this) {
      if (throwable != null) {
        throw new ShuffleError("error in shuffle in " + throwingThreadName,
                               throwable);
      }
    }
    
    return kvIter;
  }

  @Override
  public void close(){
  }

  public synchronized void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the 
      // exception immediately.
      synchronized (scheduler) {
        scheduler.notifyAll();
      }
    }
  }
  
  public static class ShuffleError extends IOException {
    private static final long serialVersionUID = 5753909320586607881L;

    ShuffleError(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
