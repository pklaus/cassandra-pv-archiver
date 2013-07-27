/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;

import com.aquenos.csstudio.archive.cassandra.EngineNameHolder;
import com.aquenos.csstudio.archive.cassandra.Sample;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CompressorPerChannelState.CompressorPerLevelState;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CompressorPerChannelState.GeneralState;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CompressorPerChannelState.NumericState;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Calculates compressed samples and deletes old samples. This class is only
 * intended for internal use by the classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class SampleCompressor {

    private static class ChannelInfo {
        CompressorPerChannelState state;
        HashMap<Long, LinkedList<Sample>> compressionLevelToSampleQueue;
        HashMap<Long, Boolean> compressionLevelToSamplesLost;
        boolean needsPeriodicRun;
        long nextPeriodicRunTime;
    }

    private enum ScheduleRequestResult {
        SUCCESS, QUEUE_IS_FULL, ALREADY_QUEUED
    }

    private final static int MAX_QUEUE_SIZE = 1024;
    private final static long PERIODIC_RUN_INTERVAL = 14400000L;

    private final Logger logger = Logger.getLogger(WriterBundle.NAME);

    private Keyspace keyspace;
    private ConsistencyLevel writeDataConsistencyLevel;
    private SampleStore sampleStore;
    private int numCompressorWorkers = 1;

    private State state = State.STOPPED;
    private final Object stateLock = new Object();

    private BlockingQueue<Sample> inboundSampleQueue = new LinkedBlockingQueue<Sample>();
    private BlockingQueue<CompressionRequest> outboundCompressionRequestQueue;
    private LinkedHashSet<String> internalChannelNameQueue = new LinkedHashSet<String>();
    private HashSet<String> unacknowledgedChannelNames = new HashSet<String>();
    private BlockingQueue<CompressionResponse> inboundCompressionResponseQueue = new LinkedBlockingQueue<CompressionResponse>();

    private ThreadGroup compressorThreadGroup;
    private Thread managementThread;
    private HashSet<Thread> workerThreads = new HashSet<Thread>();

    private long lastStatisticsTimestamp;
    private int channelCountSinceLastStatistics;

    private HashMap<String, ChannelInfo> channelInfoMap;

    private final Runnable compressionRunner = new Runnable() {
        @Override
        public void run() {
            SampleCompressor.this.run();
        }
    };

    public enum State {
        RUNNING, STOPPED, STARTING, STOPPING
    }

    public SampleCompressor(Keyspace keyspace,
            ConsistencyLevel writeDataConsistencyLevel,
            SampleStore sampleStore, int numCompressorWorkers) {
        this.keyspace = keyspace;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.sampleStore = sampleStore;
        this.compressorThreadGroup = new ThreadGroup("compressor-threads");
        this.numCompressorWorkers = numCompressorWorkers;
        // We allocate a queue that can hold ten requests per worker. This
        // ensures that the workers can continue their task when the management
        // thread is busy with some other actions. However, it is also short
        // enough to make it unlikely that a request for the same channel is
        // rescheduled, while the channel is still waiting in the outbound
        // queue.
        outboundCompressionRequestQueue = new LinkedBlockingQueue<CompressionRequest>(
                numCompressorWorkers * 10);
    }

    public void start() {
        synchronized (stateLock) {
            if (!state.equals(State.STOPPED)) {
                throw new IllegalStateException(
                        "SampleCompressor must be in stopped state to be started.");
            }
            try {
                state = State.STARTING;
                managementThread = new Thread(compressorThreadGroup,
                        compressionRunner, "compressor-management-thread");
                managementThread.start();
                for (int i = 0; i < numCompressorWorkers; i++) {
                    Thread workerThread = new Thread(compressorThreadGroup,
                            new SampleCompressorWorker(keyspace,
                                    writeDataConsistencyLevel, sampleStore,
                                    this, outboundCompressionRequestQueue,
                                    inboundCompressionResponseQueue),
                            "compressor-worker-thread-" + i);
                    workerThread.start();
                    workerThreads.add(workerThread);
                }
                state = State.RUNNING;
            } finally {
                if (!state.equals(State.RUNNING)) {
                    // Something went wrong during startup, so we order the
                    // management thread to be stopped, if it has been created.
                    if (managementThread != null) {
                        managementThread.interrupt();
                    }
                }
            }
        }
    }

    public void stop() {
        synchronized (stateLock) {
            if (state.equals(State.STOPPED)) {
                return;
            } else if (state.equals(State.STARTING)) {
                throw new IllegalStateException(
                        "The sample compressor can not be stopped while it is in starting state.");
            }
            state = State.STOPPING;
            if (managementThread != null) {
                managementThread.interrupt();
            }
            // The management thread stops all the worker threads, so we just
            // wait for the management thread here.
            while (managementThread != null && managementThread.isAlive()) {
                try {
                    // It is important that we yield the state lock while
                    // waiting. Otherwise the management thread, that also needs
                    // the state lock, could never finish.
                    stateLock.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            managementThread = null;
            state = State.STOPPED;
        }
    }

    public State getState() {
        synchronized (stateLock) {
            return state;
        }
    }

    public void queueSample(Sample sample) {
        // As the inbound queue is only restricted by the integer range, we do
        // not expect the add action to fail. Therefore we use add instead of
        // put or offer.
        inboundSampleQueue.add(sample);
    }

    private void run() {
        try {
            boolean initialized = false;
            while (!initialized && !Thread.currentThread().isInterrupted()) {
                try {
                    channelInfoMap = new HashMap<String, SampleCompressor.ChannelInfo>();
                    CassandraArchiveConfig archiveConfig = sampleStore
                            .getArchiveConfig();
                    String engineName = EngineNameHolder.getEngineName();
                    if (engineName == null) {
                        // Throwing an exception will ensure that the thread
                        // will sleep for some time and then retry to get the
                        // engine name.
                        throw new NullPointerException();
                    }
                    EngineConfig engineConfig = archiveConfig
                            .findEngine(engineName);
                    GroupConfig[] groupConfigs = archiveConfig
                            .getGroups(engineConfig);
                    for (GroupConfig groupConfig : groupConfigs) {
                        ChannelConfig[] channelConfigs = archiveConfig
                                .getChannels(groupConfig);
                        for (ChannelConfig channelConfig : channelConfigs) {
                            String channelName = channelConfig.getName();
                            channelInfoMap
                                    .put(channelName,
                                            createChannelInfo(
                                                    channelName,
                                                    archiveConfig
                                                            .findCompressionLevelConfigs(channelName)));
                        }
                    }
                    // We also want to process all disabled channels once, so
                    // that newly added compression levels are filled with data.
                    GroupConfig disabledGroupConfig = archiveConfig
                            .getDisabledChannelsGroup(engineConfig);
                    ChannelConfig[] disabledChannelConfigs = archiveConfig
                            .getChannels(disabledGroupConfig);
                    for (ChannelConfig channelConfig : disabledChannelConfigs) {
                        String channelName = channelConfig.getName();
                        ChannelInfo channelInfo = createChannelInfo(
                                channelName,
                                archiveConfig
                                        .findCompressionLevelConfigs(channelName));
                        channelInfoMap.put(channelName, channelInfo);
                    }
                    // Periodically we want to perform a compression and
                    // retention run. We only have to do this, when new data has
                    // been added, however for each channel we want to execute
                    // this task at least once, because processing might not
                    // have finished on the last run. We want to spread out this
                    // initial run over a longer time period in order to avoid
                    // peaks in CPU usage.
                    LinkedList<String> allChannels = new LinkedList<String>(
                            channelInfoMap.keySet());
                    Collections.shuffle(allChannels);
                    int channelNumber = 0;
                    double numberOfChannels = allChannels.size();
                    long currentTime = System.currentTimeMillis();
                    for (String channelName : allChannels) {
                        ChannelInfo channelInfo = channelInfoMap
                                .get(channelName);
                        long offset = Math
                                .round((channelNumber / numberOfChannels)
                                        * PERIODIC_RUN_INTERVAL);
                        channelInfo.nextPeriodicRunTime = currentTime + offset;
                        channelInfo.needsPeriodicRun = true;
                        channelNumber++;
                    }

                    // Set the initialized flag so that the loop will not be
                    // executed again.
                    initialized = true;
                } catch (Exception e) {
                    // The initialization might fail because the database is not
                    // available (yet). Therefore we wait some time before we
                    // retry.
                    try {
                        Thread.sleep(10000L);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            // Reset timestamp and counter for statistics.
            lastStatisticsTimestamp = System.currentTimeMillis();
            channelCountSinceLastStatistics = 0;
            while (!Thread.currentThread().isInterrupted()) {
                long startTime = System.currentTimeMillis();
                // Read samples from the inbound queue and add them to their
                // respective queue. If they are already listed in the internal
                // queue, this add method will have no effect.
                Sample sample;
                while ((sample = inboundSampleQueue.poll()) != null) {
                    String channelName = sample.getChannelName();
                    long compressionPeriod = sample.getCompressionPeriod();
                    ChannelInfo channelInfo = channelInfoMap.get(channelName);
                    if (channelInfo == null) {
                        CompressionLevelConfig[] compressionLevelConfigs;
                        try {
                            compressionLevelConfigs = sampleStore
                                    .getArchiveConfig()
                                    .findCompressionLevelConfigs(channelName);
                        } catch (Exception e) {
                            logger.log(Level.WARNING,
                                    "Could not get compression-level configurations for channel \""
                                            + channelName + "\".", e);
                            continue;
                        }
                        channelInfo = createChannelInfo(channelName,
                                compressionLevelConfigs);
                        channelInfoMap.put(channelName, channelInfo);
                    }
                    LinkedList<Sample> sampleQueue = channelInfo.compressionLevelToSampleQueue
                            .get(compressionPeriod);
                    channelInfo.needsPeriodicRun = true;
                    if (sampleQueue == null) {
                        // We do not need samples for this channel and
                        // compression level, thus we can just continue.
                        continue;
                    }
                    sampleQueue.add(sample);
                    while (sampleQueue.size() > MAX_QUEUE_SIZE) {
                        sampleQueue.poll();
                        channelInfo.compressionLevelToSamplesLost.put(
                                compressionPeriod, true);
                    }
                    internalChannelNameQueue.add(channelName);
                }
                // Read acknowledgments for processed channels from queue and
                // remove the from the list of unacknowledged channels. This
                // will ensure that the channels can be queued again.
                CompressionResponse response;
                while ((response = inboundCompressionResponseQueue.poll()) != null) {
                    String channelName = response.request.channelName;
                    unacknowledgedChannelNames.remove(channelName);
                    // If this was a periodic run and it failed, we have to make
                    // sure another run is scheduled.
                    if (response.request.deleteOldSamples && !response.success) {
                        ChannelInfo channelInfo = channelInfoMap
                                .get(channelName);
                        channelInfo.needsPeriodicRun = true;
                    }
                }
                // Schedule periodic processing for all channels due.
                long currentTime = System.currentTimeMillis();
                for (Map.Entry<String, ChannelInfo> entry : channelInfoMap
                        .entrySet()) {
                    String channelName = entry.getKey();
                    ChannelInfo channelInfo = entry.getValue();
                    if (unacknowledgedChannelNames.contains(channelName)) {
                        // A request for this channel has already been send to
                        // the worker queue and not been acknowledged yet, so
                        // it is not sent again.
                        continue;
                    }
                    if (channelInfo.needsPeriodicRun
                            && channelInfo.nextPeriodicRunTime <= currentTime) {
                        ScheduleRequestResult result = scheduleRequest(
                                channelName, true);
                        if (result == ScheduleRequestResult.SUCCESS) {
                            // We increment the time for the next run and set
                            // the run-needed flag to false. If another run is
                            // needed (either because data has been added to the
                            // channel or because the request failed), the flag
                            // will be set again later.
                            channelInfo.nextPeriodicRunTime += PERIODIC_RUN_INTERVAL;
                            channelInfo.needsPeriodicRun = false;
                        } else if (result == ScheduleRequestResult.QUEUE_IS_FULL) {
                            // Outbound queue is full, so we break the loop.
                            // Otherwise we could stay in this loop for a
                            // very long time.
                            break;
                        } else if (result == ScheduleRequestResult.ALREADY_QUEUED) {
                            // The channel is already queued for processing,
                            // thus we do not add it again (otherwise two
                            // threads might process the same channel).
                            continue;
                        }
                    }
                }

                // Fill the outbound queue with requests from the internal
                // queue.
                for (Iterator<String> i = internalChannelNameQueue.iterator(); i
                        .hasNext();) {
                    String nextChannelName = i.next();
                    ScheduleRequestResult result = scheduleRequest(
                            nextChannelName, false);
                    if (result == ScheduleRequestResult.SUCCESS) {
                        // Channel name has been added to the outbound
                        // queue, so we can remove it from the internal
                        // queue.
                        i.remove();
                    } else if (result == ScheduleRequestResult.QUEUE_IS_FULL) {
                        // Outbound queue is full, so we break the loop.
                        // Otherwise we could stay in this loop for a
                        // very long time.
                        break;
                    } else if (result == ScheduleRequestResult.ALREADY_QUEUED) {
                        // The channel is already queued for processing, thus we
                        // do not add it again (otherwise two threads might
                        // process the same channel).
                        continue;
                    }
                }

                // Log statistics every five minutes.
                long statisticsTimeDiff = System.currentTimeMillis()
                        - lastStatisticsTimestamp;
                if (statisticsTimeDiff >= 300000L) {
                    int channelsInQueueOrBeingProcessed = outboundCompressionRequestQueue
                            .size() + numCompressorWorkers;
                    int finishedCount = channelCountSinceLastStatistics
                            - channelsInQueueOrBeingProcessed;
                    double channelsPerSecond = ((double) finishedCount)
                            / ((double) statisticsTimeDiff) * 1000.0;
                    StringBuilder sb = new StringBuilder();
                    sb.append("The sample compressor processed ");
                    sb.append(finishedCount);
                    sb.append(" channels since the last statistics output (~ 5 minutes). ");
                    sb.append("This are ");
                    sb.append(channelsPerSecond);
                    sb.append(" channels per second on average.");
                    logger.info(sb.toString());
                    lastStatisticsTimestamp = System.currentTimeMillis();
                    // Set the number of channels excluded from the current
                    // statistics as the start for the next statistics: If these
                    // channels have been processed until the next statistics
                    // output, they should be considered as part of the result.
                    channelCountSinceLastStatistics = channelsInQueueOrBeingProcessed;
                }

                // If this run took less than 500 ms (this can only have
                // happened if our internal queue is empty), we wait some time
                // to make sure this loop does not consume all CPU resources.
                long runInterval = 500;
                long delay = runInterval
                        - (System.currentTimeMillis() - startTime);
                // We only wait if the delay is more than 10 ms.
                if (delay > 10L) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } finally {
            // When this thread is being stopped, the worker threads must also
            // be stopped. We can safely synchronize to the stateLock here,
            // because the stop method will yield the lock while waiting for
            // this thread.
            synchronized (stateLock) {
                while (!workerThreads.isEmpty()) {
                    for (Iterator<Thread> i = workerThreads.iterator(); i
                            .hasNext();) {
                        Thread workerThread = i.next();
                        if (workerThread.isAlive()) {
                            workerThread.interrupt();
                        } else {
                            i.remove();
                        }
                    }
                    if (!workerThreads.isEmpty()) {
                        try {
                            stateLock.wait(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                managementThread = null;
                state = State.STOPPED;
            }
        }
    }

    private ScheduleRequestResult scheduleRequest(String channelName,
            boolean deleteOldSamples) {
        if (unacknowledgedChannelNames.contains(channelName)) {
            // A request for this channel has already been send to
            // the worker queue and not been acknowledged yet, so
            // it is not sent again.
            return ScheduleRequestResult.ALREADY_QUEUED;
        }
        try {
            CompressionRequest request = new CompressionRequest();
            request.channelName = channelName;
            ChannelInfo channelInfo = channelInfoMap.get(channelName);
            request.compressorState = channelInfo.state;
            request.sourceSamples = channelInfo.compressionLevelToSampleQueue;
            request.sourceSamplesLost = channelInfo.compressionLevelToSamplesLost;
            request.deleteOldSamples = deleteOldSamples;
            if (outboundCompressionRequestQueue.offer(request, 500,
                    TimeUnit.MILLISECONDS)) {
                // We have to use new maps for the sample queues and
                // lost sample information, because the original
                // maps may now be used by one of the worker
                // threads.
                channelInfo.compressionLevelToSampleQueue = new HashMap<Long, LinkedList<Sample>>();
                for (long compressionLevel : request.sourceSamples.keySet()) {
                    channelInfo.compressionLevelToSampleQueue.put(
                            compressionLevel, new LinkedList<Sample>());
                }
                channelInfo.compressionLevelToSamplesLost = new HashMap<Long, Boolean>();
                // We also have to add the channel to the list of
                // unacknowledged channels, so that it will not
                // be queued again.
                unacknowledgedChannelNames.add(channelName);
                // Increase statistics counter.
                channelCountSinceLastStatistics++;
                return ScheduleRequestResult.SUCCESS;
            } else {
                return ScheduleRequestResult.QUEUE_IS_FULL;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ScheduleRequestResult.QUEUE_IS_FULL;
        }
    }

    private ChannelInfo createChannelInfo(String channelName,
            CompressionLevelConfig[] compressionLevelConfigs) {
        ChannelInfo channelInfo = new ChannelInfo();
        channelInfo.compressionLevelToSampleQueue = new HashMap<Long, LinkedList<Sample>>();
        channelInfo.compressionLevelToSamplesLost = new HashMap<Long, Boolean>();
        channelInfo.state = new CompressorPerChannelState();
        // The information needed for periodic runs is added later, thus we
        // initialize it in a way that no periodic run would be executed.
        channelInfo.needsPeriodicRun = false;
        channelInfo.nextPeriodicRunTime = Long.MAX_VALUE;
        // First we sort the compression levels in the order of their
        // compression period (from short too long). We do this, so that
        // compression levels with a longer period can use the data from
        // compression levels with a shorter period, instead of having to use
        // raw data. The raw compression level is handled separately, as it is
        // never compressed.
        LinkedList<CompressionLevelConfig> retentionOnly = new LinkedList<CompressionLevelConfig>();
        TreeMap<Long, CompressionLevelConfig> compressionPeriodToConfig = new TreeMap<Long, CompressionLevelConfig>();
        for (CompressionLevelConfig config : compressionLevelConfigs) {
            long compressionPeriod = config.getCompressionPeriod();
            if (compressionPeriod <= 0) {
                // No compression
                retentionOnly.add(config);
            } else {
                // There can never be two compression level configurations with
                // the same compression period, so this is safe.
                compressionPeriodToConfig.put(compressionPeriod, config);
            }
        }
        // We use a linked hash map so that the iteration order is retained.
        channelInfo.state.perLevelStates = new LinkedHashMap<Long, CompressorPerChannelState.CompressorPerLevelState>();
        for (CompressionLevelConfig compressionLevelConfig : compressionPeriodToConfig
                .values()) {
            long compressionPeriod = compressionLevelConfig
                    .getCompressionPeriod();
            CompressorPerLevelState levelState = new CompressorPerLevelState();
            levelState = new CompressorPerLevelState();
            levelState.generalState = new GeneralState();
            levelState.numericState = new NumericState();
            levelState.retentionPeriod = compressionLevelConfig
                    .getRetentionPeriod();
            Long nextCompressionPeriod = compressionPeriod;
            while (nextCompressionPeriod != null) {
                nextCompressionPeriod = compressionPeriodToConfig
                        .lowerKey(nextCompressionPeriod);
                if (nextCompressionPeriod != null
                        && compressionPeriod % (nextCompressionPeriod * 2) == 0) {
                    // Found a compression level with a shorter compression
                    // period, that the current compression period is an even
                    // integer multiple of. In this case, we will benefit from
                    // using that compression level instead of the raw samples,
                    // because the samples from the compression level are
                    // exactly aligned to the start and end of the intervals
                    // used to calculate the samples of this compression level.
                    break;
                }
            }
            if (nextCompressionPeriod == null) {
                // If no matching compression level is found, we have to use
                // the raw samples.
                levelState.sourceCompressionPeriod = 0L;
            } else {
                levelState.sourceCompressionPeriod = nextCompressionPeriod;
            }
            channelInfo.state.perLevelStates.put(compressionPeriod, levelState);
            channelInfo.compressionLevelToSampleQueue.put(
                    levelState.sourceCompressionPeriod,
                    new LinkedList<Sample>());
        }
        // Process remaining compression levels for retention
        for (CompressionLevelConfig compressionLevelConfig : retentionOnly) {
            boolean enableRetention = compressionLevelConfig
                    .getRetentionPeriod() > 0;
            if (enableRetention) {
                CompressorPerLevelState levelState = new CompressorPerLevelState();
                levelState.retentionPeriod = compressionLevelConfig
                        .getRetentionPeriod();
                channelInfo.state.perLevelStates.put(
                        compressionLevelConfig.getCompressionPeriod(),
                        levelState);
            }
        }
        return channelInfo;
    }

}
