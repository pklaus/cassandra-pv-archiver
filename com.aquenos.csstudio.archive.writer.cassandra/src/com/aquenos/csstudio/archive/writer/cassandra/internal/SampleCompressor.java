/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import me.prettyprint.hector.api.Keyspace;

/**
 * Calculates compressed samples and deletes old samples. This class is only
 * intended for internal use by the classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class SampleCompressor {
	private final Logger logger = Logger.getLogger(WriterBundle.NAME);

	private Keyspace keyspace;
	private int numCompressorWorkers = 1;

	private State state = State.STOPPED;
	private final Object stateLock = new Object();

	private BlockingQueue<String> inboundChannelNameQueue = new LinkedBlockingQueue<String>();
	private BlockingQueue<String> outboundChannelNameQueue;
	private LinkedHashSet<String> internalChannelNameQueue = new LinkedHashSet<String>();
	private HashSet<String> unacknowledgedChannelNames = new HashSet<String>();
	private BlockingQueue<String> acknowledgeChannelNameQueue = new LinkedBlockingQueue<String>();

	private ThreadGroup compressorThreadGroup;
	private Thread managementThread;
	private HashSet<Thread> workerThreads = new HashSet<Thread>();

	private long lastStatisticsTimestamp;
	private int channelCountSinceLastStatistics;

	private final Runnable compressionRunner = new Runnable() {
		@Override
		public void run() {
			SampleCompressor.this.run();
		}
	};

	public enum State {
		RUNNING, STOPPED, STARTING, STOPPING
	}

	public SampleCompressor(Keyspace keyspace, int numCompressorWorkers) {
		this.keyspace = keyspace;
		this.compressorThreadGroup = new ThreadGroup("compressor-threads");
		this.numCompressorWorkers = numCompressorWorkers;
		// We allocate a queue that can hold ten requests per worker. This
		// ensures that the workers can continue their task when the management
		// thread is busy with some other actions. However, it is also short
		// enough to make it unlikely that a request for the same channel is
		// rescheduled, while the channel is still waiting in the outbound
		// queue.
		outboundChannelNameQueue = new LinkedBlockingQueue<String>(
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
									outboundChannelNameQueue,
									acknowledgeChannelNameQueue),
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

	public void queueChannelProcessRequest(String channelName) {
		// As the inbound queue is only restricted by the integer range, we do
		// not expect the add action to fail. Therefore we use add instead of
		// put or offer.
		inboundChannelNameQueue.add(channelName);
	}

	public void queueChannelProcessRequests(Collection<String> channelNames) {
		// As the inbound queue is only restricted by the integer range, we do
		// not expect the add action to fail. Therefore we use add instead of
		// put or offer.
		inboundChannelNameQueue.addAll(channelNames);
	}

	private void run() {
		try {
			// Reset timestamp and counter for statistics.
			lastStatisticsTimestamp = System.currentTimeMillis();
			channelCountSinceLastStatistics = 0;
			while (!Thread.currentThread().isInterrupted()) {
				long startTime = System.currentTimeMillis();
				// Read channel names from the inbound queue and add them to the
				// internal queue. If they are already listed in the internal
				// queue, this add method will have no effect.
				String channelName;
				while ((channelName = inboundChannelNameQueue.poll()) != null) {
					internalChannelNameQueue.add(channelName);
				}
				// Read acknowledgments for processed channels from queue and
				// remove the from the list of unacknowledged channels. This
				// will ensure that the channels can be queued again.
				while ((channelName = acknowledgeChannelNameQueue.poll()) != null) {
					unacknowledgedChannelNames.remove(channelName);
				}
				// Fill the outbound queue with requests from the internal
				// queue.
				for (Iterator<String> i = internalChannelNameQueue.iterator(); i
						.hasNext();) {
					String nextChannelName = i.next();
					if (unacknowledgedChannelNames.contains(nextChannelName)) {
						// A request for this channel has already been send to
						// the worker queue and not been acknowledged yet, so
						// it is not sent again.
						continue;
					}
					try {
						if (outboundChannelNameQueue.offer(nextChannelName,
								500, TimeUnit.MILLISECONDS)) {
							// Channel name has been added to the outbound
							// queue, so we can remove it from the internal
							// queue.
							i.remove();
							// We also have to add the channel to the list of
							// unacknowledged channels, so that it will not
							// be queued again.
							unacknowledgedChannelNames.add(nextChannelName);
							// Increase statistics counter.
							channelCountSinceLastStatistics++;
						} else {
							// Outbound queue is full, so we break the loop.
							// Otherwise we could stay in this loop for a
							// very long time.
							break;
						}
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}

				// Log statistics every five minutes.
				long statisticsTimeDiff = System.currentTimeMillis()
						- lastStatisticsTimestamp;
				if (statisticsTimeDiff >= 300000L) {
					int channelsInQueueOrBeingProcessed = outboundChannelNameQueue
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

}
