/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;

import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilySamples;
import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilySamplesBucketSize;
import com.aquenos.csstudio.archive.cassandra.internal.CompressionLevelSampleStore;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.NotifyingMutationBatch;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Stores samples in the Cassandra database. In general the implementation is
 * thread-safe, however there should never be two sample stores in the whole
 * cluster (even across different JVMs) that write to the same channel.
 * 
 * @author Sebastian Marsching
 * @see Sample
 */
public class SampleStore {

    private Cluster cluster;
    private Keyspace keyspace;
    private ConsistencyLevel readDataConsistencyLevel;
    private ConsistencyLevel writeDataConsistencyLevel;
    private ConsistencyLevel readMetaDataConsistencyLevel;
    private ConsistencyLevel writeMetaDataConsistencyLevel;
    private CassandraArchiveConfig archiveConfig;
    private boolean enableWrites;
    private boolean enableCache;
    // We do not expect a high number of concurrent writes to this map, because
    // it only holds a relatively low number of entries, most of which are
    // initialized in the startup phase.
    private ConcurrentHashMap<Long, CompressionLevelSampleStore> compressionPeriodToSampleStore = new ConcurrentHashMap<Long, CompressionLevelSampleStore>();

    /**
     * Constructor. Creates a sample store whichs reads data from and writes
     * data to the passed keyspace.
     * 
     * @param cluster
     *            Cassandra cluster that stores the database.
     * @param keyspace
     *            keyspace used for accessing the database.
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
     * @param enableWrites
     *            if set to <code>true</code> the sample store will assume that
     *            it is the only instance that writes to the channels used with
     *            it. This will allow certain read caches to be enabled and
     *            certain maintenance operations to be performed.
     * @param enableCache
     *            if set to <code>true</code> the sample store will cache
     *            certain pieces of information in order to improve the read
     *            performance. This flag should only be set if all channels read
     *            through this sample store are not written to by other sample
     *            store instances. Otherwise, it can lead to stale data being
     *            returned. If set to <code>false</code> all read operations
     *            will access the database without any caching.
     */
    public SampleStore(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            boolean enableWrites, boolean enableCache) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.readDataConsistencyLevel = readDataConsistencyLevel;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.readMetaDataConsistencyLevel = readMetaDataConsistencyLevel;
        this.writeMetaDataConsistencyLevel = writeMetaDataConsistencyLevel;
        this.archiveConfig = new CassandraArchiveConfig(this.cluster,
                this.keyspace, this.readDataConsistencyLevel,
                this.writeDataConsistencyLevel,
                this.readMetaDataConsistencyLevel,
                this.writeMetaDataConsistencyLevel, this, !enableWrites);
        this.enableWrites = enableWrites;
        this.enableCache = enableCache;
        if (!this.enableWrites && this.enableCache) {
            throw new IllegalArgumentException(
                    "Caching may not be activated on a read-only sample store.");
        }
    }

    /**
     * Returns the archive configuration that is associated with this sample
     * store. This is mainly useful when caching is enabled for this sample
     * store, because then the archive configuration returned by this method
     * will also profit from this caching.
     * 
     * @return archive configuration for this sample store.
     */
    public CassandraArchiveConfig getArchiveConfig() {
        return this.archiveConfig;
    }

    /**
     * Finds samples in the database. This method actually does not fetch all
     * the samples from the database. Instead the samples are fetched in chunks
     * while iterating using the iterator provided. This means that connection
     * problems that happen after this method has returned might result in a
     * {@link RuntimeException} being thrown by the iterator's methods.
     * 
     * @param compressionPeriod
     *            compression period of the compression level to find samples
     *            for.
     * @param channelName
     *            name of the channel to find samples for.
     * @param start
     *            start timestamp. Only samples whose timestamps are greater
     *            than or equal the start timestamp are returned. If the start
     *            timestamp is <code>null</code>, the first sample returned is
     *            the first sample in the database.
     * @param end
     *            end timestamp. Only samples whose timestamps are less than or
     *            equal the end timestamp are returned. If the end timestamp is
     *            <code>null</code>, no upper limit is placed on the samples'
     *            timestamps.
     * @param numberOfSamples
     *            maximum number of samples to return. If less samples match the
     *            predicates specified by <code>start</code> and
     *            <code>end</code>, only these samples will be returned.
     *            However, this method will never return more samples than
     *            specified by this parameter. If this parameter is set to a
     *            negative number, all samples in the requested range are
     *            returned.
     * @param reverse
     *            if <code>true</code> the samples are returned in reverse order
     *            (newest first, oldest last). In this case, the
     *            <code>start</code> timestamp must be greater than or equal to
     *            the <code>end</code> timestamp. If the parameter is
     *            <code>false</code>, the samples are returned in the natural
     *            order of their timestamps.
     * @return samples matching the predicates, never <code>null</code> .
     * @throws ConnectionException
     *             if an error occurs while performing an operation in the
     *             database.
     */
    public Iterable<Sample> findSamples(long compressionPeriod,
            String channelName, ITimestamp start, ITimestamp end,
            int numberOfSamples, boolean reverse) throws ConnectionException {
        CompressionLevelSampleStore store = getSampleStoreForCompressionLevel(compressionPeriod);
        return store.findSamples(channelName, start, end, numberOfSamples,
                reverse);
    }

    /**
     * Inserts a sample into the database using the passed mutator. The sample
     * will not really be inserted until the mutators <code>execute</code>
     * method is called.
     * 
     * @param mutationBatch
     *            mutation batch to use for the insertion operation. This
     *            mutation batch must have the same consistency level that was
     *            given to this sample store's constructor as the
     *            <code>writeDataConsistencyLevel</code> parameter.
     * @param compressionPeriod
     *            compression period of the compression level to insert the
     *            sample for.
     * @param channelName
     *            name of the channel to insert the sample for.
     * @param value
     *            sample's value.
     * @throws ConnectionException
     *             if an error occurs while performing an operation in the
     *             database.
     * @throws IllegalStateException
     *             if this method is called on a sample store which does not
     *             have writes enabled.
     */
    public void insertSample(NotifyingMutationBatch mutationBatch,
            long compressionPeriod, String channelName, IValue value)
            throws ConnectionException, IllegalStateException {
        if (!enableWrites) {
            throw new IllegalStateException("This sample store is read-only.");
        }
        CompressionLevelSampleStore store = getSampleStoreForCompressionLevel(compressionPeriod);
        store.insertSample(channelName, value, mutationBatch);
    }

    /**
     * Deletes samples from the database.
     * 
     * @param compressionPeriod
     *            compression period of the compression level to delete the
     *            sample from.
     * @param channelName
     *            name of the channel to delete the sample from.
     * @param until
     *            timestamp of the last sample to be deleted (inclusive). Must
     *            not be <code>null</code>.
     * @throws ConnectionException
     *             if an error occurs while performing an operation in the
     *             database.
     * @throws IllegalStateException
     *             if this method is called on a sample store which does not
     *             have writes enabled.
     */
    public void deleteSamples(long compressionPeriod, String channelName,
            ITimestamp until) throws ConnectionException, IllegalStateException {
        if (!enableWrites) {
            throw new IllegalStateException("This sample store is read-only.");
        }
        CompressionLevelSampleStore store = getSampleStoreForCompressionLevel(compressionPeriod);
        store.deleteSamples(channelName, until);
    }

    /**
     * Checks which bucket size is currently used for the specified channel and
     * compression period. If the most recent bucket size is not zero and the
     * most recent sample is relatively old, a bucket size of zero is inserted
     * in order to avoid long search processes for newer samples. A call to this
     * method does have no effect if
     * {@link #insertSample(NotifyingMutationBatch, long, String, IValue)} has
     * been called for the same channel and compression period previously. In
     * this case however, the check has already been performed at the time the
     * first sample has been inserted.
     * 
     * @param compressionPeriod
     *            compression period to check the bucket size for.
     * @param channelName
     *            name of the channel to check the bucket size for.
     * @throws ConnectionException
     *             if an error occurs while performing an operation in the
     *             database.
     * @throws IllegalStateException
     *             if this method is called on a sample store which does not
     *             have writes enabled.
     */
    public void verifyZeroBucketSize(long compressionPeriod, String channelName)
            throws ConnectionException, IllegalStateException {
        if (!enableWrites) {
            throw new IllegalStateException("This sample store is read-only.");
        }
        CompressionLevelSampleStore store = getSampleStoreForCompressionLevel(compressionPeriod);
        store.verifyZeroBucketSize(channelName);
    }

    /**
     * Performs a clean-up of the column-families used to store samples. During
     * the clean-up operation, all samples which have a
     * channel/compression-level combination not listed in
     * <code>compressionLevelNames</code> are deleted. This includes raw samples
     * (compression period equals zero). In order to not delete raw samples, a
     * pair of ("channel name", 0) has to be present for each channel.
     * 
     * @param compressionLevelPeriods
     *            set of pairs, where the first entry of each pair is the name
     *            of a channel and the second entry a compression period.
     * @param printStatus
     *            if set to <code>true</code>, for each sample being deleted a
     *            message is printed to the standard output.
     * @throws ConnectionException
     *             if an error occurs while performing an operation in the
     *             database.
     * @throws IllegalStateException
     *             if this method is called on a sample store which does not
     *             have writes enabled.
     */
    public void performCleanUp(Set<Pair<String, Long>> compressionLevelPeriods,
            boolean printStatus) throws ConnectionException,
            IllegalStateException {
        if (!enableWrites) {
            throw new IllegalStateException("This sample store is read-only.");
        }
        // We find all compression levels in the database by finding the
        // corresponding column families.
        TreeSet<Long> compressionPeriodsInDatabase = new TreeSet<Long>();
        String cfSamplesPrefix = ColumnFamilySamples.NAME_PREFIX + "_";
        for (ColumnFamilyDefinition cfDef : keyspace.describeKeyspace()
                .getColumnFamilyList()) {
            String cfName = cfDef.getName();
            if (cfName.startsWith(cfSamplesPrefix)) {
                cfName = cfName.substring(cfSamplesPrefix.length());
                compressionPeriodsInDatabase.add(Long.valueOf(cfName));
            } else if (cfName.equals(ColumnFamilySamples.NAME_PREFIX)) {
                compressionPeriodsInDatabase.add(0L);
            }
        }
        // We make a list of all the compression levels that are available in
        // the configuration and for each compression level we make a list of
        // the channels, which are available for this compression level.
        TreeMap<Long, TreeSet<String>> compressionPeriodsInConfiguration = new TreeMap<Long, TreeSet<String>>();
        for (Pair<String, Long> pair : compressionLevelPeriods) {
            String channelName = pair.getFirst();
            long compressionPeriod = pair.getSecond();
            TreeSet<String> channels = compressionPeriodsInConfiguration
                    .get(compressionPeriod);
            if (channels == null) {
                channels = new TreeSet<String>();
                compressionPeriodsInConfiguration.put(compressionPeriod,
                        channels);
            }
            channels.add(channelName);
        }
        for (long compressionPeriod : compressionPeriodsInDatabase) {
            if (!compressionPeriodsInConfiguration
                    .containsKey(compressionPeriod)) {
                // The compression level is not defined in the configuration,
                // thus we can drop the corresponding column families.
                if (printStatus) {
                    System.out
                            .println("There are no channels for the compression period of "
                                    + compressionPeriod
                                    + " seconds, thus the corresponding column families are being dropped.");
                }
                ColumnFamilySamples cfSamples = new ColumnFamilySamples(
                        compressionPeriod);
                ColumnFamilySamplesBucketSize cfSamplesBucketSize = new ColumnFamilySamplesBucketSize(
                        compressionPeriod);
                keyspace.dropColumnFamily(cfSamples.getCF());
                keyspace.dropColumnFamily(cfSamplesBucketSize.getCF());
            } else {
                CompressionLevelSampleStore store = getSampleStoreForCompressionLevel(compressionPeriod);
                HashSet<String> channelNames = new HashSet<String>();
                for (Pair<String, Long> pair : compressionLevelPeriods) {
                    if (compressionPeriod == pair.getSecond()) {
                        channelNames.add(pair.getFirst());
                    }
                }
                store.performCleanUp(channelNames, printStatus);
            }
        }
    }

    /**
     * Returns the timestamp of the latest sample for the given channel or the
     * latest bucket size for this channel (if it is more recent than the latest
     * sample). Returns <code>null</code> if no sample and bucket size can be
     * found. Please not that the value returned might not necessarily represent
     * the timestamp of a sample. Instead, it could just be the timestamp of a
     * bucket size. However, for technical reasons, this has to be treated like
     * there was a sample with this timestamp (e.g. no samples with timestamps
     * older than this timestamp can be inserted).
     * 
     * @param compressionPeriod
     *            compression period of the compression level to find the newest
     *            sample's timestamp for.
     * @param channelName
     *            name of the channel to get the last timestamp for.
     * @return timestamp of the newest sample or bucket size or
     *         <code>null</code> if no sample is found.
     * @throws ConnectionException
     *             if an error occurs while performing an operation in the
     *             database.
     */
    public ITimestamp getLastSampleTimestamp(long compressionPeriod,
            String channelName) throws ConnectionException {
        CompressionLevelSampleStore store = getSampleStoreForCompressionLevel(compressionPeriod);
        return store.getLastSampleTimestamp(channelName);
    }

    private CompressionLevelSampleStore getSampleStoreForCompressionLevel(
            long compressionPeriod) {
        CompressionLevelSampleStore store = compressionPeriodToSampleStore
                .get(compressionPeriod);
        if (store == null) {
            if (compressionPeriod < 0L) {
                throw new IllegalArgumentException(
                        "Compression period must not be negative.");
            }
            store = new CompressionLevelSampleStore(cluster, keyspace,
                    readDataConsistencyLevel, writeDataConsistencyLevel,
                    readMetaDataConsistencyLevel,
                    writeMetaDataConsistencyLevel, archiveConfig,
                    compressionPeriod, enableWrites, enableCache);
            CompressionLevelSampleStore previousStore = compressionPeriodToSampleStore
                    .putIfAbsent(compressionPeriod, store);
            if (previousStore != null) {
                return previousStore;
            }
        }
        return store;
    }
}
