/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra;

import java.util.UUID;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.writer.ArchiveWriter;
import org.csstudio.archive.writer.WriteChannel;
import org.csstudio.data.values.IDoubleValue;
import org.csstudio.data.values.IEnumeratedValue;
import org.csstudio.data.values.ILongValue;
import org.csstudio.data.values.IStringValue;
import org.csstudio.data.values.IValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aquenos.csstudio.archive.cassandra.Sample;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.NotifyingMutationBatch;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.WrappedNotifyingMutationBatch;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CassandraWriteChannel;
import com.aquenos.csstudio.archive.writer.cassandra.internal.SampleCompressor;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Writer for the Cassandra archive.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveWriter implements ArchiveWriter {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private NotifyingMutationBatch mutationBatch;

    private AstyanaxContext<Cluster> context;
    private Cluster cluster;
    private Keyspace keyspace;
    private ConsistencyLevel readDataConsistencyLevel;
    private ConsistencyLevel writeDataConsistencyLevel;
    private ConsistencyLevel readMetaDataConsistencyLevel;
    private ConsistencyLevel writeMetaDataConsistencyLevel;

    private SampleStore sampleStore;
    private CassandraArchiveConfig archiveConfig;
    private EngineConfig engineConfig;
    private SampleCompressor sampleCompressor;

    /**
     * Creates a Cassandra archive writer using the specified parameters.
     * 
     * @param cassandraHosts
     *            host or comma-seperated list of hosts that provide the
     *            Cassandra database.
     * @param cassandraPort
     *            TCP port to contact the Cassandra server(s) on.
     * @param keyspaceName
     *            name of the keyspace in the Cassandra database.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param retryPolicy
     *            retry policy to user.
     * @param username
     *            username to use for authentication or <code>null</code> to use
     *            no authentication.
     * @param password
     *            password to use for authentication or <code>null</code> to use
     *            no authentication.
     * @throws ConnectionException
     *             if connection to keyspace fails.
     */
    public CassandraArchiveWriter(String cassandraHosts, int cassandraPort,
            String cassandraKeyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            RetryPolicy retryPolicy, String username, String password)
            throws ConnectionException {
        this(cassandraHosts, cassandraPort, cassandraKeyspace,
                readDataConsistencyLevel, writeDataConsistencyLevel,
                readMetaDataConsistencyLevel, writeMetaDataConsistencyLevel,
                retryPolicy, username, password, 0);
    }

    /**
     * Creates a Cassandra archive reader using the specified parameters. Offers
     * the option to run the sample compressor for the engine. Only one instance
     * of the sample compressor should exist for a single engine at any point in
     * time.
     * 
     * @param cassandraHosts
     *            host or comma-seperated list of hosts that provide the
     *            Cassandra database.
     * @param cassandraPort
     *            TCP port to contact the Cassandra server(s) on.
     * @param keyspaceName
     *            name of the keyspace in the Cassandra database.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param retryPolicy
     *            retry policy to use.
     * @param username
     *            username to use for authentication or <code>null</code> to use
     *            no authentication.
     * @param password
     *            password to use for authentication or <code>null</code> to use
     *            no authentication.
     * @param numCompressorWorkers
     *            the number of worker threads created for periodically
     *            performing the compression for all channels managed by the
     *            specified engine. If this is less than one, the compression is
     *            deactivated.
     * @throws ConnectionException
     *             if connection to keyspace fails.
     */
    protected CassandraArchiveWriter(String cassandraHosts, int cassandraPort,
            String keyspaceName, ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            RetryPolicy retryPolicy, String username, String password,
            int numCompressorWorkers) throws ConnectionException {
        // We use a random identifier for the cluster, because there might be
        // several instances of the archive reader. If we used the same
        // cluster for all of them, a cluster still being used might be shutdown
        // when one of them is closed.
        String uuid = UUID.randomUUID().toString();
        ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl(
                uuid).setSeeds(cassandraHosts).setPort(cassandraPort);
        if (username != null && password != null) {
            cpConfig.setAuthenticationCredentials(new SimpleAuthenticationCredentials(
                    username, password));
        }
        AstyanaxConfiguration asConfig = new AstyanaxConfigurationImpl()
                .setDefaultReadConsistencyLevel(readDataConsistencyLevel)
                .setDefaultWriteConsistencyLevel(writeDataConsistencyLevel)
                .setRetryPolicy(retryPolicy)
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE);
        this.context = new AstyanaxContext.Builder().forCluster(uuid)
                .forKeyspace(keyspaceName)
                .withConnectionPoolConfiguration(cpConfig)
                .withAstyanaxConfiguration(asConfig)
                .buildCluster(ThriftFamilyFactory.getInstance());
        this.context.start();
        Cluster cluster = this.context.getClient();
        initialize(cluster, cluster.getKeyspace(keyspaceName),
                readDataConsistencyLevel, writeDataConsistencyLevel,
                readMetaDataConsistencyLevel, writeMetaDataConsistencyLevel,
                numCompressorWorkers);
    }

    /**
     * Creates a Cassandra archive reader using the passed keyspace to access
     * the database. The connection to the database is not closed when the
     * {@link #close()} method is called.
     * 
     * @param cluster
     *            Cassandra cluster that stores the database.
     * @param keyspace
     *            keyspace that stores the configuration and samples.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     */
    public CassandraArchiveWriter(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel) {
        initialize(cluster, keyspace, readDataConsistencyLevel,
                writeDataConsistencyLevel, readMetaDataConsistencyLevel,
                writeMetaDataConsistencyLevel, 0);
    }

    /**
     * Creates a Cassandra archive reader using the passed keyspace to access
     * the database. The connection to the database is not closed when the
     * {@link #close()} method is called. Offers the option to run the sample
     * compressor for the engine. Only one instance of the sample compressor
     * should exist for a single engine at any point in time.
     * 
     * @param cluster
     *            Cassandra cluster that stores the database.
     * @param keyspace
     *            keyspace that stores the configuration and samples.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param numCompressorWorkers
     *            the number of worker threads created for periodically
     *            performing the compression for all channels managed by the
     *            specified engine. If this is less than one, the compression is
     *            deactivated.
     */
    protected CassandraArchiveWriter(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            int numCompressorWorkers) {
        initialize(cluster, keyspace, readDataConsistencyLevel,
                writeDataConsistencyLevel, readMetaDataConsistencyLevel,
                writeMetaDataConsistencyLevel, numCompressorWorkers);
    }

    private void initialize(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            int numCompressorWorkers) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.readDataConsistencyLevel = readDataConsistencyLevel;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.readMetaDataConsistencyLevel = readMetaDataConsistencyLevel;
        this.writeMetaDataConsistencyLevel = writeMetaDataConsistencyLevel;
        this.sampleStore = new SampleStore(this.cluster, this.keyspace,
                this.readDataConsistencyLevel, this.writeDataConsistencyLevel,
                this.readMetaDataConsistencyLevel,
                this.writeMetaDataConsistencyLevel, true, true);
        this.archiveConfig = this.sampleStore.getArchiveConfig();
        if (numCompressorWorkers > 0) {
            this.sampleCompressor = new SampleCompressor(this.keyspace,
                    this.writeDataConsistencyLevel, this.sampleStore,
                    numCompressorWorkers);
            this.sampleCompressor.start();
        }
    }

    private void initializeMutationBatch() {
        if (mutationBatch == null) {
            mutationBatch = new WrappedNotifyingMutationBatch(keyspace
                    .prepareMutationBatch().withConsistencyLevel(
                            writeDataConsistencyLevel));
        }
    }

    private void processDisabledChannels() {
        try {
            GroupConfig disabledChannelsGroup = archiveConfig
                    .getDisabledChannelsGroup(engineConfig);
            for (ChannelConfig channelConfig : archiveConfig
                    .getChannels(disabledChannelsGroup)) {
                String channelName = channelConfig.getName();
                // We want to make sure that a zero bucket size is inserted if
                // the most recent sample for the channel is very old.
                // Therefore we call SampleStore#verifyZeroBucketSize(...) once
                // for each compression level.
                sampleStore.verifyZeroBucketSize(0L, channelName);
                for (CompressionLevelConfig compressionLevelConfig : archiveConfig
                        .findCompressionLevelConfigs(channelName)) {
                    sampleStore.verifyZeroBucketSize(
                            compressionLevelConfig.getCompressionPeriod(),
                            channelName);
                }
            }
        } catch (Exception e) {
            // Processing the deactivated channels is not so important that we
            // want an error to stop the getChannel(name) operation. Therefore
            // we just log the error.
            logger.error("Error while trying to process deactivated channels: "
                    + e.getMessage(), e);
        }

    }

    @Override
    public WriteChannel getChannel(String name) throws Exception {
        // We want to make sure that a zero bucket size is inserted if the most
        // recent sample for the channel is very old. Therefore we call
        // SampleStore#verifyZeroBucketSize(...) once for each compression
        // level.
        sampleStore.verifyZeroBucketSize(0L, name);
        for (CompressionLevelConfig compressionLevelConfig : archiveConfig
                .findCompressionLevelConfigs(name)) {
            sampleStore.verifyZeroBucketSize(
                    compressionLevelConfig.getCompressionPeriod(), name);
        }
        // We need to have the engine configuration. Unfortunately the archive
        // engine does not provide us with the engine name. Therefore we
        // reconstruct the engine name by looking which engine the channels
        // belong to.
        ChannelConfig channelConfig = archiveConfig.findChannel(name);
        if (channelConfig != null) {
            String engineName = archiveConfig.getEngineName(channelConfig);
            if (this.engineConfig == null) {
                this.engineConfig = archiveConfig.findEngine(engineName);
                // Make sure that deactivated channels are processed once.
                processDisabledChannels();
            } else {
                if (!this.engineConfig.getName().equals(engineName)) {
                    throw new IllegalStateException(
                            "Got request for channel \""
                                    + name
                                    + "\" which belongs to engine \""
                                    + engineConfig.getName()
                                    + "\", but already got requests for channels belonging to engine \""
                                    + this.engineConfig.getName()
                                    + "\" earlier. However, this archiver writer only supports being used with a single engine.");
                }
            }
        }
        return new CassandraWriteChannel(name);
    }

    @Override
    public void addSample(WriteChannel channel, IValue sample) throws Exception {
        // Check that we got the right (our) channel implementation, because
        // we will cast it later.
        if (!(channel instanceof CassandraWriteChannel)) {
            throw new Exception("Got instance of "
                    + channel.getClass().getName()
                    + ", but expected instance of "
                    + CassandraWriteChannel.class.getName());
        }
        // Check that seconds part of time is greater than or equal to zero.
        if (sample.getTime().seconds() < 0L) {
            throw new Exception(
                    "Seconds port of timestamp must be greater than or equal to zero, but is "
                            + sample.getTime().seconds());
        }
        // Check that nanoseconds part of time is within expected range, because
        // we will cast it to a 32-bit integer later.
        long nanoSeconds = sample.getTime().nanoseconds();
        if (nanoSeconds < 0L || nanoSeconds >= 1000000000L) {
            throw new Exception(
                    "Nanoseconds part of timestamp must be between zero and one billion but is "
                            + nanoSeconds + " for channel " + channel.getName());
        }
        // Check that value is of a supported type, because we will cast it
        // later.
        if (!(sample instanceof IDoubleValue
                || sample instanceof IEnumeratedValue
                || sample instanceof ILongValue || sample instanceof IStringValue)) {
            throw new Exception("Sample value for channel " + channel.getName()
                    + " is of unsupported type " + sample.getClass().getName());
        }
        // Make sure that we have a mutation batch.
        initializeMutationBatch();
        // Insert the sample.
        String channelName = channel.getName();
        sampleStore.insertSample(mutationBatch, 0L, channelName, sample);
        if (sampleCompressor != null) {
            // Add channel to sample compressor queue.
            sampleCompressor.queueSample(new Sample(0L, channelName, sample));
        }
    }

    @Override
    public void flush() throws Exception {
        // Execute the mutation batch in order to send the inserts to the
        // database.
        boolean success = false;
        try {
            if (mutationBatch != null) {
                mutationBatch.execute();
            }
            success = true;
        } finally {
            if (!success) {
                // If there was a problem we should clean up the mutation batch
                // so that a new one will be used for the next inserts.
                mutationBatch = null;
            }
        }
    }

    @Override
    public void close() {
        if (sampleCompressor != null) {
            sampleCompressor.stop();
            sampleCompressor = null;
        }
        if (context != null) {
            context.shutdown();
            context = null;
        }
    }

}
