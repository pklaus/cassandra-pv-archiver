/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.aquenos.cassandra.pvarchiver.controlsystem.SampleListener;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessArchivingChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;

/**
 * <p>
 * Sample writer delegate that limits the (minimum and maximum) rate at which
 * samples are written. Depending on the <code>minUpdatePeriod</code> and
 * <code>maxUpdatePeriod</code> {@linkplain ConfigurationOptions configuration
 * options}, this writer drops samples that arrive to quickly or repeats samples
 * (with an updated time stamp) when no new samples arrives for a (too) long
 * time.
 * </p>
 * 
 * <p>
 * This class is intended for internal use by
 * {@link ChannelAccessArchivingChannel} and its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class LimitingSampleWriterDelegate implements SampleWriterDelegate {

    private final ChannelAccessArchivingChannel channel;
    private final Runnable checkRunnable = new Runnable() {
        @Override
        public void run() {
            runCheck();
        }
    };
    private boolean destroyed;
    private ChannelAccessSample lastReceivedSample;
    private int lastReceivedSampleEstimatedSize;
    private boolean lastReceivedSampleHasBeenWritten;
    private long lastWrittenSampleClockTime;
    private final Object lock;
    private final long maxUpdatePeriodMilliseconds;
    private final long minUpdatePeriodMilliseconds;
    private final SampleListener<ChannelAccessSample> sampleListener;
    private final TreeSet<Long> scheduledCheckClockTimes;
    private final ScheduledExecutorService scheduledExecutor;

    private static ChannelAccessSample updateSampleTimeStamp(
            ChannelAccessSample originalSample, long newTimeStampMilliseconds) {
        long newTimeStamp = newTimeStampMilliseconds * 1000000L;
        switch (originalSample.getType()) {
        case AGGREGATED_SCALAR_CHAR:
        case AGGREGATED_SCALAR_DOUBLE:
        case AGGREGATED_SCALAR_FLOAT:
        case AGGREGATED_SCALAR_LONG:
        case AGGREGATED_SCALAR_SHORT:
            return new ChannelAccessAggregatedSample(newTimeStamp,
                    originalSample.getType(),
                    ((ChannelAccessUdtSample) originalSample).getValue());
        case ARRAY_CHAR:
        case ARRAY_DOUBLE:
        case ARRAY_ENUM:
        case ARRAY_FLOAT:
        case ARRAY_LONG:
        case ARRAY_SHORT:
        case ARRAY_STRING:
        case SCALAR_CHAR:
        case SCALAR_DOUBLE:
        case SCALAR_ENUM:
        case SCALAR_FLOAT:
        case SCALAR_LONG:
        case SCALAR_SHORT:
        case SCALAR_STRING:
            return new ChannelAccessRawSample(newTimeStamp,
                    originalSample.getType(),
                    ((ChannelAccessUdtSample) originalSample).getValue(),
                    ((ChannelAccessRawSample) originalSample)
                            .isOriginalSample());
        case DISABLED:
            return new ChannelAccessDisabledSample(newTimeStamp,
                    ((ChannelAccessDisabledSample) originalSample)
                            .isOriginalSample());
        case DISCONNECTED:
            return new ChannelAccessDisconnectedSample(newTimeStamp,
                    ((ChannelAccessDisconnectedSample) originalSample)
                            .isOriginalSample());
        default:
            throw new RuntimeException("Unhandled sample type: "
                    + originalSample.getType());
        }
    }

    /**
     * Creates the sample writer. The created sample writer writes samples using
     * the specified <code>sampleListener</code>, passing the specified
     * <code>channel</code>. The <code>configurationOptions</code> are used for
     * determining the minimum and maximum rate at which samples are supposed to
     * be written. The specified <code>scheduledExecutor</code> is used for
     * scheduling delayed checks needed to implement this functionality.
     * 
     * @param channel
     *            channel for which this sample-writer delegate writes samples.
     *            This is the channel instance that is passed to the
     *            <code>sampleListener</code>.
     * @param configurationOptions
     *            configuration options for the channel. The minimum and maximum
     *            update rate are read from these configuration options.
     * @param sampleListener
     *            sample listener which is used for actually writing the samples
     *            into the archive.
     * @param scheduledExecutor
     *            scheduled executor that is used for scheduling tasks related
     *            to the mechanism for controlling the sample rate. This sample
     *            writer needs to schedule such tasks in order to check whether
     *            a previously written sample should be written again (when
     *            there is an upper limit on the update period) or whether a
     *            sample that was not written earlier, should be written now (if
     *            there is a lower limit on the update rate).
     */
    public LimitingSampleWriterDelegate(ChannelAccessArchivingChannel channel,
            ConfigurationOptions configurationOptions,
            SampleListener<ChannelAccessSample> sampleListener,
            ScheduledExecutorService scheduledExecutor) {
        this.channel = channel;
        this.lastWrittenSampleClockTime = -1L;
        this.lock = new Object();
        this.sampleListener = sampleListener;
        this.scheduledCheckClockTimes = new TreeSet<Long>();
        this.scheduledExecutor = scheduledExecutor;
        // We convert the update period to milliseconds. We use the system clock
        // for dealing with the update period and System.currentTimeMillis()
        // uses milliseconds, so by doing the conversion now, we only have to do
        // it once. If the specified value is not zero but less than a
        // millisecond, we use a value of one millisecond because round to a
        // value of zero would lead to totally different behavior due to the
        // special meaning of that value.
        long maxUpdatePeriodNanoseconds = configurationOptions
                .getMaxUpdatePeriod();
        if (maxUpdatePeriodNanoseconds == 0L) {
            this.maxUpdatePeriodMilliseconds = 0L;
        } else {
            if (maxUpdatePeriodNanoseconds < 1000000L) {
                this.maxUpdatePeriodMilliseconds = 1L;
            } else {
                if (maxUpdatePeriodNanoseconds % 1000000L < 500000L) {
                    this.maxUpdatePeriodMilliseconds = maxUpdatePeriodNanoseconds / 1000000L;
                } else {
                    this.maxUpdatePeriodMilliseconds = maxUpdatePeriodNanoseconds / 1000000L + 1L;
                }
            }
        }
        long minUpdatePeriodNanoseconds = configurationOptions
                .getMinUpdatePeriod();
        if (minUpdatePeriodNanoseconds == 0L) {
            this.minUpdatePeriodMilliseconds = 0L;
        } else {
            if (minUpdatePeriodNanoseconds < 1000000L) {
                this.minUpdatePeriodMilliseconds = 1L;
            } else {
                if (minUpdatePeriodNanoseconds % 1000000L < 500000L) {
                    this.minUpdatePeriodMilliseconds = minUpdatePeriodNanoseconds / 1000000L;
                } else {
                    this.minUpdatePeriodMilliseconds = minUpdatePeriodNanoseconds / 1000000L + 1L;
                }
            }
        }
    }

    @Override
    public void destroy() {
        synchronized (lock) {
            destroyed = true;
        }
    }

    @Override
    public void writeSample(ChannelAccessSample sample, int estimatedSampleSize) {
        long nextCheckDelay;
        boolean shouldWriteSample = true;
        synchronized (lock) {
            long nextCheckClockTime = -1L;
            // When the channel (and thus this sample writer) has been
            // destroyed, we should not run any further actions. In particular,
            // we should not schedule any more checks.
            if (destroyed) {
                return;
            }
            long currentClockTime = System.currentTimeMillis();
            // Even if we do not schedule another check, we want to clean up our
            // set of scheduled checks in order to reclaim memory as early as
            // possible.
            cleanUpScheduledCheckClockTimes(currentClockTime);
            // First, we check whether the sample arrived before the lower limit
            // of the update period had passed and should thus not be written
            // yet.
            if (minUpdatePeriodMilliseconds != 0L) {
                if (lastWrittenSampleClockTime >= 0L
                        && currentClockTime - lastWrittenSampleClockTime < minUpdatePeriodMilliseconds) {
                    // When the sample is not written now, we might want to
                    // write it later if no other sample arrives in the
                    // meantime. However, we never schedule more than one check
                    // in order to avoid queuing a large number of jobs.
                    nextCheckClockTime = lastWrittenSampleClockTime
                            + minUpdatePeriodMilliseconds;
                    shouldWriteSample = false;
                } else {
                    lastWrittenSampleClockTime = currentClockTime;
                }
            }
            // Second, we schedule a check when the sample has been written and
            // we have an upper limit for the update period. We only schedule
            // this check if we have not scheduled a run for the minimum update
            // period check which always happens earlier than this check.
            if (maxUpdatePeriodMilliseconds != 0L && shouldWriteSample
                    && nextCheckClockTime < 0L) {
                nextCheckClockTime = currentClockTime
                        + maxUpdatePeriodMilliseconds;
            }
            lastReceivedSample = sample;
            lastReceivedSampleEstimatedSize = estimatedSampleSize;
            lastReceivedSampleHasBeenWritten = shouldWriteSample;
            if (shouldWriteSample) {
                lastWrittenSampleClockTime = currentClockTime;
            }
            // We want to convert the clock time to a delay. As a side effect,
            // this also tests whether another, earlier check has already been
            // scheduled and returns a negative number in this case.
            nextCheckDelay = addToScheduledCheckClockTimes(currentClockTime,
                    nextCheckClockTime);
        }
        // We call external code after releasing the mutex. This way there is no
        // risk of a dead lock.
        if (nextCheckDelay >= 0L) {
            scheduledExecutor.schedule(checkRunnable, nextCheckDelay,
                    TimeUnit.MILLISECONDS);
        }
        if (shouldWriteSample) {
            sampleListener.onSampleReceived(channel, sample,
                    estimatedSampleSize);
        }
    }

    private long addToScheduledCheckClockTimes(long currentClockTime,
            long nextCheckClockTime) {
        assert (Thread.holdsLock(lock));
        if (nextCheckClockTime < 0L) {
            return -1L;
        }
        // When a scheduled check is delayed significantly, it can happen that
        // the theoretical time for the next check is before the current time.
        // In this case, we want to run the next check immediately.
        if (nextCheckClockTime < currentClockTime) {
            nextCheckClockTime = currentClockTime;
        }
        // Remove all entries for checks that must already have run. This code
        // is needed in order to avoid a memory leak.
        cleanUpScheduledCheckClockTimes(currentClockTime);
        // If a check before the designated time has already been scheduled, we
        // do not want to schedule another check. This is important because a
        // channel that receives updates at a very high rate with a typical
        // update period that is small compared to the configured minimum update
        // period, we would schedule a lot of checks, occupying memory in the
        // scheduler and more importantly calling the runCheck method far more
        // often than it makes sense.
        Long boxedNextCheckClockTime = nextCheckClockTime;
        if (scheduledCheckClockTimes.floor(boxedNextCheckClockTime) != null) {
            return -1L;
        }
        scheduledCheckClockTimes.add(boxedNextCheckClockTime);
        return nextCheckClockTime - currentClockTime;
    }

    private void cleanUpScheduledCheckClockTimes(long currentClockTime) {
        assert (Thread.holdsLock(lock));
        scheduledCheckClockTimes.headSet(currentClockTime, true).clear();
    }

    private void runCheck() {
        long nextCheckDelay;
        ChannelAccessSample writeSample = null;
        int writeSampleEstimatedSize = 0;
        synchronized (lock) {
            long nextCheckClockTime = -1L;
            // When the channel (and thus this sample writer) has been
            // destroyed, we should not run any further actions. In particular,
            // we should not schedule any more checks.
            if (destroyed) {
                return;
            }
            long currentClockTime = System.currentTimeMillis();
            // There are two kinds of situations that we have to handle: The
            // last sample has not been written because it arrived before the
            // minimum update period had passed or it was written, but it is now
            // time to write it again. In addition to these two cases, it is
            // possible that the check has been scheduled, but is not necessary
            // any longer because another sample has been written in between.
            // A check might be executed later than scheduled, but never
            // earlier, so we can assume that the point in time for which we
            // have been waiting has always passed.
            if (minUpdatePeriodMilliseconds != 0L
                    && !lastReceivedSampleHasBeenWritten) {
                // If the time for writing the sample has come, we write it.
                // Otherwise, we schedule the next check at the point in time
                // when we should write the sample.
                if (lastWrittenSampleClockTime >= 0L
                        && currentClockTime - lastWrittenSampleClockTime < minUpdatePeriodMilliseconds) {
                    nextCheckClockTime = lastWrittenSampleClockTime
                            + minUpdatePeriodMilliseconds;
                } else {
                    lastReceivedSampleHasBeenWritten = true;
                    lastWrittenSampleClockTime = lastWrittenSampleClockTime
                            + minUpdatePeriodMilliseconds;
                    writeSample = lastReceivedSample;
                    writeSampleEstimatedSize = lastReceivedSampleEstimatedSize;
                }
            }
            if (maxUpdatePeriodMilliseconds != 0L) {
                // When more time has passed since writing the last sample than
                // specified by the maximum update period, we repeat the last
                // sample using an updated time-stamp. Otherwise, we schedule
                // the next check at the point in time when this condition is
                // going to be fulfilled. We do not schedule such a check if
                // a check for the minimum update period is going to be
                // scheduled because that check will always run earlier than the
                // check we would schedule here.
                if (lastWrittenSampleClockTime + maxUpdatePeriodMilliseconds <= currentClockTime) {
                    writeSample = updateSampleTimeStamp(lastReceivedSample,
                            lastWrittenSampleClockTime
                                    + maxUpdatePeriodMilliseconds);
                    // The size of the sample does not change by updating its
                    // time stamp.
                    writeSampleEstimatedSize = lastReceivedSampleEstimatedSize;
                    lastWrittenSampleClockTime = lastWrittenSampleClockTime
                            + maxUpdatePeriodMilliseconds;
                } else if (nextCheckClockTime < 0L) {
                    nextCheckClockTime = lastWrittenSampleClockTime
                            + maxUpdatePeriodMilliseconds;
                }
                // When no other check has been scheduled and we are going to
                // write a sample, we schedule a check so that the sample can
                // be repeated when the maximum update period is exceeded.
                // We can safely write to nextCheckClockTime because if we have
                // a sample, the minimum update period check has not scheduled
                // another check.
                if (writeSample != null) {
                    nextCheckClockTime = lastWrittenSampleClockTime
                            + maxUpdatePeriodMilliseconds;
                }
            }
            // We want to convert the clock time to a delay. As a side effect,
            // this also tests whether another, earlier check has already been
            // scheduled and returns a negative number in this case.
            nextCheckDelay = addToScheduledCheckClockTimes(currentClockTime,
                    nextCheckClockTime);
        }
        // We call external code after releasing the mutex. This way there
        // is no risk of a dead lock.
        if (nextCheckDelay >= 0L) {
            scheduledExecutor.schedule(checkRunnable, nextCheckDelay,
                    TimeUnit.MILLISECONDS);
        }
        if (writeSample != null) {
            sampleListener.onSampleReceived(channel, writeSample,
                    writeSampleEstimatedSize);
        }
    }

}
