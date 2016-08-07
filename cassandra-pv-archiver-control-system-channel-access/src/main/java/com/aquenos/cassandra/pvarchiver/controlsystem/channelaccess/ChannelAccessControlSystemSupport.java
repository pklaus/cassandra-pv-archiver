/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketState;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleDecimator;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleListener;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleWithSizeEstimate;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessDatabaseAccess;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessDisabledSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessSampleDecimator;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessSampleValueAccess;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.CommonsLoggingErrorHandler;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ConfigurationOptions;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.SampleSizeEstimator;
import com.aquenos.epics.jackie.client.ChannelAccessClient;
import com.aquenos.epics.jackie.client.ChannelAccessClientConfiguration;
import com.aquenos.epics.jackie.client.DefaultChannelAccessClient;
import com.aquenos.epics.jackie.common.exception.ErrorHandler;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * Control-system support for Channel Access (CA) based control systems. Channel
 * Access is the protocol used by EPICS 3.
 * </p>
 * 
 * <p>
 * This control-system support has a number of configuration options. When
 * specified on the server-level (in the server's configuration file), these
 * options are used as defaults when an option is not specified explicitly for a
 * channel. All options in the configuration file use the "channelAccess" prefix
 * (e.g. "controlSystem.channelAccess.monitorMask"). When specified for a
 * channel, the option name has to be used without a prefix.
 * </p>
 * 
 * <table summary="list of options for the Channel Access control-system support">
 * <tr>
 * <th>Option Name</th>
 * <th>Default Value</th>
 * <th>Description</th>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>clockSource</code></td>
 * <td><code>prefer_origin</code></td>
 * <td>Clock source used when archiving samples. Allowed values are
 * <code>local</code>, <code>origin</code>, and <code>prefer_origin</code>. When
 * using <code>local</code>, the time stamp sent with the value by the Channel
 * Access server is always discarded and the local clock (of the archiving
 * server) is used instead. When using <code>origin</code>, the time stamp
 * provided by the Channel Access server is used and the whole sample is
 * discarded when the <code>maxClockSkew</code> is exceeded. When
 * <code>prefer_origin</code>, the time stamp from the Channel Access server is
 * normally used, but it is replaced with a time stamp from the local clock when
 * the <code>maxClockSkew</code> is exceeded.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>enablingChannel</code></td>
 * <td><i>not set</i></td>
 * <td>Channel that decides whether archiving is enabled or disabled. If the
 * enabling channel is not connected, archiving is disabled. If the enabling
 * channel is connected, archiving is enabled depending on the enabling
 * channel's value. When the enabling channel's value is of an integer type, the
 * target channel is enabled if the enabling channel's value is non-zero. If the
 * enabling channel's value is of a floating-point type, the target channel is
 * enabled when the enabling channel's value is neither zero nor not-a-number.
 * When the enabling channel's value is of a string type, the target channel is
 * enabled when the enabling channel's value is neither the empty string, nor
 * "0", "false", "no", or "off". Leading and trailing white-space is ignored for
 * this comparison and the comparison is not case sensitive.<br>
 * If this option is empty, archiving is always enabled. If a channel has been
 * disabled in the archiving configuration, this option does not have any effect
 * and archiving always stays disabled, even if an enabling channel is specified
 * and indicates that archiving should be enabled.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>maxClockSkew</code></td>
 * <td>30.0</td>
 * <td>Maximum clock skew (in seconds). The specified value must be a finite,
 * non-negative floating point number. If zero, the clock skew check is
 * disabled, which mean that the time stamp provided by the Channel Access
 * server is always used (unless the <code>clockSource</code> is set to
 * <code>local</code>). If not zero, the clock skew specifies the maximum
 * absolute difference that is allowed between the local clock and the time
 * stamp provided with a value by the Channel Access Server. When this limit is
 * exceeded, the exact behavior depends on the <code>clockSource</code> option.
 * When the <code>clockSource</code> option is set to <code>origin</code>, the
 * sample is discarded. When it is set to <code>prefer_origin</code>, the time
 * stamp is replaced with the time from the local clock. When it is
 * <code>local</code>, the <code>maxClockSkew</code> option does not have any
 * effect.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>maxUpdatePeriod</code></td>
 * <td>0.0</td>
 * <td>Maximum period between writing two samples (in seconds). The specified
 * value must be a finite, non-negative floating point number. If zero, the
 * maximum time between two samples is unbounded, meaning that a sample is only
 * written when a it arrives from the server. Due to processing delays, the
 * actual period between writing the two samples might be slightly greater than
 * the specified period. For obvious reasons, this time stamp is always
 * generated using the local system clock, regardless of the
 * <code>clockSource</code> setting. Please see the notes below before setting a
 * non-zero value for this option.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>metaDataMonitorMask</code></td>
 * <td><code>property</code></td>
 * <td>Monitor event mask used when monitoring channels for meta-data changes.
 * This mask can be overridden for individual channels. The following tokens
 * (separated by commas, pipes, or spaces) are supported: <code>value</code>,
 * <code>archive</code>, <code>alarm</code>, <code>property</code>. Please refer
 * to the Channel Access Reference Manual for details about the meaning of this
 * mask bits. The event mask used when monitoring a channel for value changes is
 * specified separately through the <code>monitorMask</code> option.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>minUpdatePeriod</code></td>
 * <td>0.0</td>
 * <td>Minimum period between writing two samples (in seconds). The specified
 * value must be a finite, non-negative floating point number. If samples arrive
 * more quickly than the specified by the minimum update period, samples are
 * dropped in order to get the effective update period above the specified
 * threshold. If zero, the minimum time between two samples is unbounded,
 * meaning that each sample that arrives from the server is written, even when
 * samples arrive rapidly. However, for very high update rates, samples might
 * still be lost if the system cannot process them as quickly as they arrive.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>monitorMask</code></td>
 * <td><code>archive|alarm</code></td>
 * <td>Monitor event mask used when monitoring channels for value changes. This
 * mask can be overridden for individual channels. The following tokens
 * (separated by commas, pipes, or spaces) are supported: <code>value</code>,
 * <code>archive</code>, <code>alarm</code>, <code>property</code>. Please refer
 * to the Channel Access Reference Manual for details about the meaning of this
 * mask bits. The event mask used when monitoring a channel for meta-data
 * changes is specified separately through the <code>metaDataMonitorMask</code>
 * option.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>writeSampleWhenDisabled</code></td>
 * <td><code>false</code></td>
 * <td>Flag indicating whether a special marker sample should be written when a
 * channel is disabled. Possible values are <code>true</code> or
 * <code>false</code>. Please see the notes below before enabling this option.</td>
 * </tr>
 * <tr style="vertical-align: top;">
 * <td><code>writeSampleWhenDisconnected</code></td>
 * <td><code>false</code></td>
 * <td>Flag indicating whether a special marker sample should be written when a
 * channel is disconnected. Possible values are <code>true</code> or
 * <code>false</code>. Please see the notes below before enabling this option.</td>
 * </tr>
 * </table>
 * 
 * <strong>Caution when using the maxUpdatePeriod, writeSampleWhenDisabled, or
 * writeSampleWhenDisconnected options</strong>
 * 
 * <p>
 * If the <code>writeSampleWhenDisabled</code> option is set to
 * <code>true</code>, a special sample acting as a marker for the disabled state
 * is written to the archive when a channel is disabled (because it is disabled
 * in the archive configuration or because a condition for enabling archiving is
 * not met). This way, one can tell from the archived data whether a value
 * simply did not change for an extended period of time or no samples where
 * written because archiving was disabled.
 * </p>
 * 
 * <p>
 * Similarly, if the <code>writeSampleWhenDisconnected</code> option is set to
 * <code>true</code>, a special sample acting as a marker for the disconnected
 * state is written to the archive when a channel disconnects (or cannot be
 * connected when the channel is initialized). This way, one can tell from the
 * archived data whether a value simply did not change for an extended period of
 * time or no updates could be received because the channel was not connected at
 * all.
 * </p>
 * 
 * <p>
 * Specifying a non-zero value for the <code>maxUpdatePeriod</code> option has
 * the effect that a sample is repeated with an updated time-stamp when no other
 * sample is received within the specified period of time. This can help in
 * identifying whether a channel's value simply did not change or the server was
 * not running. However, decimated samples do not reflect this information,
 * meaning that decimated samples will look the same regardless of whether there
 * are no new samples for a prolonged period of time or whether the samples have
 * the same value. This means that this information will only be visible in the
 * raw samples.
 * </p>
 * 
 * <p>
 * For obvious reasons, there is no time from the origin when writing such a
 * sample. Therefore, the local system-time on the archiving server is used for
 * the sample's time-stamp. This has two possible effects: If the clock on the
 * origin server is skewed into the future, the disconnect marker sample might
 * be discarded because it will get a time stamp that is less than the time
 * stamp of the most recent sample that has already been written. If the clock
 * on the origin server is skewed into the past, the sample received when the
 * channel reconnects might have a time stamp that is less than the time stamp
 * of the disconnect marker sample and might thus be discarded, giving the
 * impression that the channel was disabled or disconnected longer than it
 * actually was and losing samples.
 * </p>
 * 
 * <p>
 * A similar problem might arise if the clocks are well-synchronized but the
 * channel's time-stamp is only updated rarely (e.g. only when the channel's
 * value changes in the backing hardware) and thus the sample sent when the
 * channel reconnects has still the old time-stamp. In this case, no sample are
 * lost, but from the archived data of the channel will look like the channel
 * has been disabled or disconnected for a longer time (until a sample with a
 * new time-stamp is sent).
 * </p>
 * 
 * <p>
 * For these reasons, this configuration option should only be enabled if the
 * channel either uses the archive server's local system-clock as a time-stamp
 * source or if the clocks are well synchronized and the channel's time-stamp is
 * updated frequently.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessControlSystemSupport implements
        ControlSystemSupport<ChannelAccessSample> {

    /**
     * Prefix used for configuration options in the server configuration.
     */
    public static final String CONFIGURATION_OPTION_PREFIX = "channelAccess";

    /**
     * Identifier uniquely identifying this control-system support. This is the
     * value returned by {@link #getId()}.
     */
    public static final String IDENTIFIER = "channel_access";

    /**
     * Human-readable name of this control-system support.
     */
    public static final String NAME = "Channel Access";

    private ChannelAccessClient client;
    private ChannelAccessDatabaseAccess databaseAccess;
    private ChannelAccessSampleValueAccess sampleValueAccess;
    private ScheduledExecutorService scheduledExecutor;
    private ConfigurationOptions serverOptions;

    /**
     * Creates an instance of the Channel Access control-system support. Each
     * instance is backed by its own {@link ChannelAccessClient}. Most of the
     * Channel Access client's configuration is initialized from environment
     * variables (<code>EPICS_CA_*</code>). Please refer to the EPICS Jackie
     * manual or the Channel Access Reference Manual for details.
     * 
     * @param configurationOptions
     *            configuration options for this control-system support. Please
     *            refer to this class's documentation for available
     *            configuration options.
     * @param session
     *            Cassandra session used for database operations.
     */
    public ChannelAccessControlSystemSupport(
            Map<String, String> configurationOptions, Session session) {
        // The ConfigurationOptions constructor will automatically check all
        // options and throw an IllegalArgumentException if one has an invalid
        // value.
        this.serverOptions = new ConfigurationOptions(configurationOptions);
        ErrorHandler errorHandler = new CommonsLoggingErrorHandler();
        // We use the defaults for most options which will result in the
        // corresponding environment variables being used.
        ChannelAccessClientConfiguration configuration = new ChannelAccessClientConfiguration(
                null, null, null, null, null, null, null, errorHandler, null,
                null);
        this.client = new DefaultChannelAccessClient(configuration);
        this.sampleValueAccess = new ChannelAccessSampleValueAccess(session);
        this.databaseAccess = new ChannelAccessDatabaseAccess(session,
                this.sampleValueAccess);
        // We prefer a scheduled executor instead of a timer because this way we
        // can have more than one thread. We create daemon threads because we do
        // not want these threads to block the shutdown of the JVM.
        // We do not use the executor for transforming futures, so we do not
        // have to set our own RejectedExecutionHandler.
        // We use the number of available processors divided by two as the
        // number of threads. This is somehow inaccurate (for example, we might
        // create too many threads for processors with hyper threading), but
        // this should not hurt us too much. Even if we detect only a single
        // processor, we want to create at least two threads so that in case of
        // a blocking thread (even though we do not expect any block operations)
        // some other tasks still have a chance to run. On the other hand, we
        // limit the pool size to 8 threads so that we do not create too many
        // threads on a machine with a very large number of cores.
        int numberOfPoolThreads = Runtime.getRuntime().availableProcessors() / 2;
        numberOfPoolThreads = Math.max(numberOfPoolThreads, 2);
        numberOfPoolThreads = Math.min(numberOfPoolThreads, 8);
        ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true).build();
        this.scheduledExecutor = Executors.newScheduledThreadPool(
                numberOfPoolThreads, daemonThreadFactory);
    }

    @Override
    public ListenableFuture<ChannelAccessArchivingChannel> createChannel(
            String channelName, Map<String, String> options,
            SampleBucketId currentBucketId,
            SampleListener<ChannelAccessSample> sampleListener) {
        try {
            // The ConfigurationOptions constructor throws an
            // IllegalArgumentException if one of the configuration options has
            // an invalid value.
            ConfigurationOptions channelOptions = new ConfigurationOptions(
                    options, serverOptions);
            // For Channel Access, creating a channel is non-blocking, so we can
            // do it right here.
            return Futures.immediateFuture(new ChannelAccessArchivingChannel(
                    channelName, channelOptions, sampleListener,
                    sampleValueAccess, client, scheduledExecutor));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public SampleDecimator<ChannelAccessSample> createSampleDecimator(
            String channelName, Map<String, String> options,
            long intervalStartTime, long intervalLength) {
        return new ChannelAccessSampleDecimator(channelName, intervalStartTime,
                intervalLength, sampleValueAccess);
    }

    @Override
    public ListenableFuture<Void> deleteSamples(SampleBucketId bucketId) {
        return databaseAccess.deleteSamples(bucketId);
    }

    @Override
    public void destroy() {
        client.destroy();
        // We only use the executor for scheduling tasks that are not critical
        // if not run at all. If we used it to transform futures, we would have
        // to run remaining tasks that are due.
        scheduledExecutor.shutdownNow();
    }

    @Override
    public ListenableFuture<SampleWithSizeEstimate<ChannelAccessSample>> generateChannelDisabledSample(
            String channelName, Map<String, String> options,
            SampleBucketId currentBucketId) {
        try {
            // The ConfigurationOptions constructor throws an
            // IllegalArgumentException if one of the configuration options has
            // an invalid value.
            ConfigurationOptions channelOptions = new ConfigurationOptions(
                    options, serverOptions);
            if (channelOptions.isWriteSampleWhenDisabled()) {
                return Futures
                        .immediateFuture(new SampleWithSizeEstimate<ChannelAccessSample>(
                                new ChannelAccessDisabledSample(System
                                        .currentTimeMillis() * 1000000L, true),
                                SampleSizeEstimator
                                        .estimateDisabledSampleSize()));
            } else {
                return Futures.immediateFuture(null);
            }
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public String getId() {
        return IDENTIFIER;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ListenableFuture<SampleBucketState> getSampleBucketState(
            SampleBucketId bucketId) {
        return databaseAccess.getSampleBucketState(bucketId);
    }

    @Override
    public ListenableFuture<ObjectResultSet<ChannelAccessSample>> getSamples(
            SampleBucketId bucketId, long timeStampGreaterThanOrEqualTo,
            long timeStampLessThanOrEqualTo, int limit) {
        return databaseAccess.getSamples(bucketId,
                timeStampGreaterThanOrEqualTo, timeStampLessThanOrEqualTo,
                limit, false);
    }

    @Override
    public ListenableFuture<ObjectResultSet<ChannelAccessSample>> getSamplesInReverseOrder(
            SampleBucketId bucketId, long timeStampGreaterThanOrEqualTo,
            long timeStampLessThanOrEqualTo, int limit) {
        return databaseAccess.getSamples(bucketId,
                timeStampGreaterThanOrEqualTo, timeStampLessThanOrEqualTo,
                limit, true);
    }

    @Override
    public void serializeSampleToJsonV1(ChannelAccessSample sample,
            JsonGenerator jsonGenerator) throws IOException {
        sampleValueAccess.serializeSampleToJsonV1(jsonGenerator, sample);

    }

    @Override
    public ListenableFuture<Void> writeSample(ChannelAccessSample sample,
            SampleBucketId bucketId, int newBucketSize) {
        return databaseAccess.writeSample(sample, bucketId, newBucketSize);
    }

}
