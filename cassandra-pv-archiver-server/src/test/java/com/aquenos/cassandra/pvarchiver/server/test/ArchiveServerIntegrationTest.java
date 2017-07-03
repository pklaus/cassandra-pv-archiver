/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.common.SimpleObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.AbstractStatefulSampleDecimator;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannelStatus;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketState;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleDecimator;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleListener;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleWithSizeEstimate;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessServiceImpl;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCacheImpl;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.archiving.TimeStampLimitMode;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.cluster.ServerOnlineStatusEvent;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.test.ControlSystemSupportRegistryStubImpl;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.SampleBucketInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAOImpl;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAOImpl;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAOInitializedEvent;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.spring.ServerProperties;
import com.aquenos.cassandra.pvarchiver.server.spring.ThrottlingProperties;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.aquenos.cassandra.pvarchiver.tests.EmbeddedCassandraServer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Integration tests that cover the core archiving operation. The tests check
 * basic operation of the {@link ArchiveConfigurationService}, the
 * {@link ArchivingService}, and the {@link ArchiveAccessServiceImpl}. Instead
 * of testing the individual components, they are plugged together and their
 * working together is tested. For this reason, these tests also (indirectly)
 * cover parts of the {@link ClusterManagementService} and the
 * {@link ChannelInformationCacheImpl}.
 * </p>
 * 
 * <p>
 * Only the operation of a single server is tested. For this reason, the
 * {@link InterNodeCommunicationService} as well as the various web interfaces
 * are not covered by the tests.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ArchiveServerIntegrationTest {

    /**
     * Stub implementation of the {@link CassandraProvider} interface that uses
     * an {@link EmbeddedCassandraServer}.
     * 
     * @author Sebastian Marsching
     */
    private static class TestCassandraProvider implements CassandraProvider {

        private Cluster cluster;
        private Session session;

        public TestCassandraProvider() {
            session = EmbeddedCassandraServer.getSession();
            cluster = session.getCluster();
        }

        @Override
        public Cluster getCluster() {
            return cluster;
        }

        @Override
        public ListenableFuture<Cluster> getClusterFuture() {
            return Futures.immediateFuture(cluster);
        }

        @Override
        public boolean isInitialized() {
            return true;
        }

        @Override
        public Session getSession() {
            return session;
        }

        @Override
        public ListenableFuture<Session> getSessionFuture() {
            return Futures.immediateFuture(session);
        }

    }

    /**
     * Stub implementation of the {@link ControlSystemChannel} interface. This
     * implementation uses static fields in the
     * {@link ArchiveServerIntegrationTest} for storing information about how it
     * has been used. This information can then be used for testing whether the
     * expected calls have been made.
     * 
     * @author Sebastian Marsching
     */
    private static class TestControlSystemChannel
            implements ControlSystemChannel {

        private String channelName;

        public TestControlSystemChannel(String channelName) {
            this.channelName = channelName;
            channelCreated.put(channelName, true);
        }

        @Override
        public void destroy() {
            channelDestroyed.put(channelName, true);
        }

        @Override
        public String getChannelName() {
            return channelName;
        }

        @Override
        public ControlSystemChannelStatus getStatus() {
            ControlSystemChannelStatus status = channelStatus.get(channelName);
            if (status == null) {
                return ControlSystemChannelStatus.disconnected();
            } else {
                return status;
            }
        }
    }

    /**
     * Stub implementation of the {@link ControlSystemSupport} interface. This
     * implementation returns {@link TestControlSystemChannel}s and stores all
     * data in memory by writing to static fields in the
     * {@link ArchiveServerIntegrationTest} class.
     * 
     * @author Sebastian Marsching
     */
    private static class TestControlSystemSupport
            implements ControlSystemSupport<TestSample> {

        @Override
        public ListenableFuture<? extends ControlSystemChannel> createChannel(
                String channelName, Map<String, String> options,
                SampleBucketId currentBucketId,
                SampleListener<TestSample> sampleListener) {
            TestControlSystemChannel channel = new TestControlSystemChannel(
                    channelName);
            channels.put(channelName, channel);
            sampleListeners.put(channelName, sampleListener);
            return Futures.immediateFuture(channel);
        }

        @Override
        public SampleDecimator<TestSample> createSampleDecimator(
                String channelName, Map<String, String> options,
                long intervalStartTime, long intervalLength) {
            return new TestSampleDecimator(channelName, intervalStartTime,
                    intervalLength);
        }

        @Override
        public ListenableFuture<Void> deleteSamples(SampleBucketId bucketId) {
            sampleBuckets.remove(bucketId);
            return Futures.immediateFuture(null);
        }

        @Override
        public void destroy() {
        }

        @Override
        public ListenableFuture<SampleWithSizeEstimate<TestSample>> generateChannelDisabledSample(
                String channelName, Map<String, String> options,
                SampleBucketId currentBucketId) {
            return Futures.immediateFuture(
                    new SampleWithSizeEstimate<TestSample>(new TestSample(
                            System.currentTimeMillis() * 1000000L, true), 100));
        }

        @Override
        public String getId() {
            return "test";
        }

        @Override
        public String getName() {
            return "Internal Test Support";
        }

        @Override
        public ListenableFuture<SampleBucketState> getSampleBucketState(
                SampleBucketId bucketId) {
            TestSampleBucket bucket = sampleBuckets.get(bucketId);
            if (bucket == null) {
                return Futures.immediateFuture(new SampleBucketState(0, 0L));
            } else {
                synchronized (bucket) {
                    return Futures.immediateFuture(new SampleBucketState(
                            bucket.size, bucket.samples.lastKey()));
                }
            }
        }

        @Override
        public ListenableFuture<ObjectResultSet<TestSample>> getSamples(
                SampleBucketId bucketId, long timeStampGreaterThanOrEqualTo,
                long timeStampLessThanOrEqualTo, int limit) {
            return Futures.<ObjectResultSet<TestSample>> immediateFuture(
                    SimpleObjectResultSet.fromIterable(getSamplesInternal(
                            bucketId, timeStampGreaterThanOrEqualTo,
                            timeStampLessThanOrEqualTo, limit, false)));
        }

        @Override
        public ListenableFuture<ObjectResultSet<TestSample>> getSamplesInReverseOrder(
                SampleBucketId bucketId, long timeStampGreaterThanOrEqualTo,
                long timeStampLessThanOrEqualTo, int limit) {
            return Futures.<ObjectResultSet<TestSample>> immediateFuture(
                    SimpleObjectResultSet.fromIterable(getSamplesInternal(
                            bucketId, timeStampGreaterThanOrEqualTo,
                            timeStampLessThanOrEqualTo, limit, true)));
        }

        @Override
        public void serializeSampleToJsonV1(TestSample sample,
                JsonGenerator jsonGenerator) throws IOException {
            // This method is not needed for our tests.
        }

        @Override
        public ListenableFuture<Void> writeSample(TestSample sample,
                SampleBucketId bucketId, int newBucketSize) {
            TestSampleBucket bucket = getOrCreateSampleBucket(bucketId);
            synchronized (bucket) {
                bucket.samples.put(sample.getTimeStamp(), sample);
                bucket.size = newBucketSize;
            }
            return Futures.immediateFuture(null);
        }

        private ImmutableList<TestSample> getSamplesInternal(
                SampleBucketId bucketId, long timeStampGreaterThanOrEqualTo,
                long timeStampLessThanOrEqualTo, int limit, boolean reverse) {
            TestSampleBucket bucket = sampleBuckets.get(bucketId);
            if (bucket == null) {
                return ImmutableList.of();
            } else {
                synchronized (bucket) {
                    // We have to avoid concurrent modifications, so we simply
                    // copy the relevant subset.
                    NavigableMap<Long, TestSample> subMap = bucket.samples
                            .subMap(timeStampGreaterThanOrEqualTo, true,
                                    timeStampLessThanOrEqualTo, true);
                    if (reverse) {
                        subMap = subMap.descendingMap();
                    }

                    Iterable<TestSample> selectedSamples = subMap.values();
                    if (limit >= 0) {
                        selectedSamples = Iterables.limit(selectedSamples,
                                limit);
                    }
                    return ImmutableList.copyOf(selectedSamples);
                }
            }
        }

    }

    /**
     * Stub implementation of the {@link InterNodeCommunicationService}
     * interface. Most methods in this implementation throw an
     * {@link UnsupportedOperationException}. Only the methods neeeded by the
     * {@link ArchiveConfigurationService} are implemented, but they do not
     * actually perform any actions.
     * 
     * @author Sebastian Marsching
     */
    private static class TestInterNodeCommunicationService
            implements InterNodeCommunicationService {

        @Override
        public ListenableFuture<List<ChannelStatus>> getArchivingStatus(
                String targetBaseUrl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<ChannelStatus> getArchivingStatusForChannel(
                String targetBaseUrl, String channelName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Long> getCurrentSystemTime(
                String targetBaseUrl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInitialized() {
            // This stub implementation does not have any dependencies, so it is
            // always ready for operation.
            return true;
        }

        @Override
        public ListenableFuture<List<ArchiveConfigurationCommandResult>> runArchiveConfigurationCommands(
                String targetBaseUrl,
                List<? extends ArchiveConfigurationCommand> commands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> updateChannelInformation(
                String targetBaseUrl,
                List<ChannelInformation> channelInformationUpdates,
                List<String> missingChannels, List<UUID> forwardToServers,
                long timeStamp) {
            // We have to support this method because it is called when the
            // configuration changes. As long as we only test a single server,
            // we can simply do nothing.
            return Futures.immediateFuture(null);
        }

    }

    /**
     * Stub implementation of the {@link Sample} interface. This implementation
     * is used by the {@link TestControlSystemSupport}. It does not store any
     * actual data but mainly provides a time stamp. When representing a
     * decimated sample, it stores the source samples that were used for
     * building the decimated samples.
     * 
     * @author Sebastian Marsching
     */
    private static class TestSample implements Sample {

        public final boolean disabled;
        public List<Triple<Long, Long, TestSample>> sourceSamples;
        public final long timeStamp;

        public TestSample(long timeStamp) {
            this(timeStamp, false);
        }

        public TestSample(long timeStamp, boolean disabled) {
            this.timeStamp = timeStamp;
            this.disabled = disabled;
        }

        @Override
        public long getTimeStamp() {
            return timeStamp;
        }

    }

    /**
     * In-memory implementation of a sample bucket. This implementation is used
     * by {@link TestControlSystemSupport} in order to store samples.
     * 
     * @author Sebastian Marsching
     */
    private static class TestSampleBucket {

        public int size;
        public TreeMap<Long, TestSample> samples = new TreeMap<Long, TestSample>();

    }

    /**
     * Stub implementation of the {@link SampleDecimator} interface. This
     * implementation is used by the {@link TestControlSystemSupport} and does
     * not perform any actual decimation. Instead, it simply creates a new
     * sample that contains references to all the source samples that have been
     * passed to the decimator. This way, it can be tested that a decimated
     * sample is actually built from the expected source samples.
     * 
     * @author Sebastian Marsching
     */
    private static class TestSampleDecimator
            extends AbstractStatefulSampleDecimator<TestSample> {

        private LinkedList<Triple<Long, Long, TestSample>> sourceSamples = new LinkedList<Triple<Long, Long, TestSample>>();

        public TestSampleDecimator(String channelName, long intervalStartTime,
                long intervalLength) {
            super(channelName, intervalStartTime, intervalLength);
        }

        @Override
        public TestSample getDecimatedSample() {
            TestSample sample = new TestSample(getIntervalStartTime());
            sample.sourceSamples = Collections.unmodifiableList(sourceSamples);
            return sample;
        }

        @Override
        public int getDecimatedSampleEstimatedSize() {
            return 100;
        }

        @Override
        protected void buildDecimatedSampleInternal() {
            // We can simply generate the resulting sample in the
            // getDecimatedSample() method.
        }

        @Override
        protected void processSampleInternal(TestSample sample, long startTime,
                long endTime) {
            sourceSamples.add(Triple.of(startTime, endTime, sample));
        }

    }

    private static final long ONE_BILLION = 1000000000L;
    private static final UUID SERVER_UUID = UUID.randomUUID();

    private static ArchiveAccessServiceImpl archiveAccessService;
    private static ArchiveConfigurationService archiveConfigurationService;
    private static ArchivingService archivingService;
    private static CassandraProvider cassandraProvider;
    private static ConcurrentHashMap<String, Boolean> channelCreated = new ConcurrentHashMap<String, Boolean>();
    private static ConcurrentHashMap<String, Boolean> channelDestroyed = new ConcurrentHashMap<String, Boolean>();
    private static ChannelInformationCacheImpl channelInformationCache;
    private static ChannelMetaDataDAOImpl channelMetaDataDAO;
    private static ConcurrentHashMap<String, TestControlSystemChannel> channels = new ConcurrentHashMap<String, ArchiveServerIntegrationTest.TestControlSystemChannel>();
    private static ConcurrentHashMap<String, ControlSystemChannelStatus> channelStatus = new ConcurrentHashMap<String, ControlSystemChannelStatus>();
    private static ClusterManagementService clusterManagementService;
    private static ClusterServersDAOImpl clusterServersDAO;
    private static ControlSystemSupport<TestSample> controlSystemSupport;
    private static ControlSystemSupportRegistry controlSystemSupportRegistry;
    private static InterNodeCommunicationService interNodeCommunicationService;
    private static ConcurrentHashMap<SampleBucketId, TestSampleBucket> sampleBuckets = new ConcurrentHashMap<SampleBucketId, TestSampleBucket>();
    private static ConcurrentHashMap<String, SampleListener<TestSample>> sampleListeners = new ConcurrentHashMap<String, SampleListener<TestSample>>();
    private static ServerProperties serverProperties;
    private static ThrottlingProperties throttlingProperties;

    /**
     * Prepares the components that are needed for running the tests. In
     * particular, this method creates the {@link ArchiveConfigurationService},
     * {@link ArchivingService}, {@link ArchiveAccessService}, and
     * {@link ChannelInformationCache} that are tested. It also creates the
     * components that are needed by these components, replacing some of them
     * with stub implementations.
     * 
     * @throws Exception
     *             if there is an error while creating or initializing the
     *             components.
     */
    @BeforeClass
    public static void prepareComponents() throws Exception {
        // We set the general log-level to INFO but reduce the log level for the
        // Cassandra server and driver to WARN because otherwise these classes
        // generate a lot of log messages and it it is hard to spot the
        // interesting ones (even warnings) between them.
        LoggingSystem loggingSystem = LoggingSystem
                .get(ArchiveServerIntegrationTest.class.getClassLoader());
        loggingSystem.setLogLevel("", LogLevel.INFO);
        loggingSystem.setLogLevel("com.datastax.driver", LogLevel.WARN);
        loggingSystem.setLogLevel("org.apache.cassandra", LogLevel.WARN);
        // Many components need an application event publisher, so we create it
        // first. This code is not thread safe, because the notified components
        // are not available yet. However, it will not be used before they are
        // and the code that use the publisher is only executed in threads that
        // see the initialized components, so it is okay for our tests.
        ApplicationEventPublisher applicationEventPublisher = new ApplicationEventPublisher() {
            @Override
            public void publishEvent(Object event) {
                if (event instanceof ClusterServersDAOInitializedEvent) {
                    ClusterServersDAOInitializedEvent clusterServersDAOInitializedEvent = (ClusterServersDAOInitializedEvent) event;
                    clusterManagementService
                            .onClusterServersDAOInitializedEvent(
                                    clusterServersDAOInitializedEvent);
                }
                if (event instanceof ServerOnlineStatusEvent) {
                    ServerOnlineStatusEvent serverOnlineStatusEvent = (ServerOnlineStatusEvent) event;
                    archivingService
                            .onServerOnlineStatusEvent(serverOnlineStatusEvent);
                    channelInformationCache
                            .onServerOnlineStatusEvent(serverOnlineStatusEvent);
                }
            }

            @Override
            public void publishEvent(ApplicationEvent event) {
                publishEvent((Object) event);
            }
        };
        // First, we initialize our internal test implementations because they
        // do not have any external dependencies.
        cassandraProvider = new TestCassandraProvider();
        controlSystemSupport = new TestControlSystemSupport();
        controlSystemSupportRegistry = new ControlSystemSupportRegistryStubImpl(
                controlSystemSupport);
        interNodeCommunicationService = new TestInterNodeCommunicationService();
        // Next, we initialize the remaining components in the order determined
        // by their dependency graph.
        channelMetaDataDAO = new ChannelMetaDataDAOImpl();
        channelMetaDataDAO.setCassandraProvider(cassandraProvider);
        clusterServersDAO = new ClusterServersDAOImpl();
        clusterServersDAO
                .setApplicationEventPublisher(applicationEventPublisher);
        clusterServersDAO.setCassandraProvider(cassandraProvider);
        serverProperties = new ServerProperties();
        serverProperties.setUuid(SERVER_UUID.toString());
        serverProperties.afterPropertiesSet();
        throttlingProperties = new ThrottlingProperties();
        clusterManagementService = new ClusterManagementService();
        clusterManagementService
                .setApplicationEventPublisher(applicationEventPublisher);
        clusterManagementService.setClusterServersDAO(clusterServersDAO);
        clusterManagementService
                .setControlSystemSupportRegistry(controlSystemSupportRegistry);
        clusterManagementService.setInterNodeCommunicationService(
                interNodeCommunicationService);
        clusterManagementService.setServerProperties(serverProperties);
        clusterManagementService.afterPropertiesSet();
        channelInformationCache = new ChannelInformationCacheImpl();
        channelInformationCache.setChannelMetaDataDAO(channelMetaDataDAO);
        channelInformationCache
                .setClusterManagementService(clusterManagementService);
        channelInformationCache.setInterNodeCommunicationService(
                interNodeCommunicationService);
        channelInformationCache.afterPropertiesSet();
        archiveAccessService = new ArchiveAccessServiceImpl();
        archiveAccessService
                .setChannelInformationCache(channelInformationCache);
        archiveAccessService.setChannelMetaDataDAO(channelMetaDataDAO);
        archiveAccessService
                .setControlSystemSupportRegistry(controlSystemSupportRegistry);
        archivingService = new ArchivingService();
        archivingService.setArchiveAccessService(archiveAccessService);
        archivingService.setCassandraProvider(cassandraProvider);
        archivingService.setChannelMetaDataDAO(channelMetaDataDAO);
        archivingService.setClusterManagementService(clusterManagementService);
        archivingService
                .setControlSystemSupportRegistry(controlSystemSupportRegistry);
        archivingService.setServerProperties(serverProperties);
        archivingService.setThrottlingProperties(throttlingProperties);
        archivingService.afterPropertiesSet();
        archiveConfigurationService = new ArchiveConfigurationService();
        archiveConfigurationService.setArchivingService(archivingService);
        archiveConfigurationService.setCassandraProvider(cassandraProvider);
        archiveConfigurationService
                .setChannelInformationCache(channelInformationCache);
        archiveConfigurationService.setChannelMetaDataDAO(channelMetaDataDAO);
        archiveConfigurationService
                .setClusterManagementService(clusterManagementService);
        archiveConfigurationService
                .setControlSystemSupportRegistry(controlSystemSupportRegistry);
        archiveConfigurationService.setInterNodeCommunicationService(
                interNodeCommunicationService);
        archiveConfigurationService.setServerProperties(serverProperties);
        archiveConfigurationService.afterPropertiesSet();
        // After initializing all components, we want to run the final
        // initialization that might start asynchronous tasks. We do not have to
        // send an event from the CassandraProvider because it was already
        // available before we started regular bean initialization.
        // We process the DAOs before the other components because the other
        // components depend on them. If we initialized the other components
        // first, their initialization would fail initially and only complete
        // after a delay, delaying the whole test process.
        channelMetaDataDAO.afterSingletonsInstantiated();
        clusterServersDAO.afterSingletonsInstantiated();
        archivingService.afterSingletonsInstantiated();
        channelInformationCache.afterSingletonsInstantiated();
        clusterManagementService.afterSingletonsInstantiated();
        // It might take some time for some of the components to initialize
        // completely. For this reason, we wait for the components to be ready.
        // This way, we should be able to avoid failing tests becomes of
        // incompletely initialized components.
        boolean initialized = false;
        long deadline = System.currentTimeMillis() + 60000L;
        while (System.currentTimeMillis() < deadline) {
            if (clusterManagementService.isOnline()) {
                initialized = true;
                break;
            }
            Thread.sleep(50L);
        }
        // If the components are still not initialized completely, this must be
        // an error.
        assertTrue(initialized);
    }

    private static TestSampleBucket getOrCreateSampleBucket(
            SampleBucketId bucketId) {
        TestSampleBucket bucket = sampleBuckets.get(bucketId);
        if (bucket == null) {
            sampleBuckets.putIfAbsent(bucketId, new TestSampleBucket());
            bucket = sampleBuckets.get(bucketId);
        }
        return bucket;
    }

    private static void verifySourceSampleTimeStamps(
            Sample actualDecimatedSample, long... expectedTimeStamps) {
        // We cast the type here so that we do not have to do this cast at every
        // point where we call this method.
        TestSample decimatedSample = (TestSample) actualDecimatedSample;
        assertEquals("Number of source samples does not match",
                expectedTimeStamps.length,
                decimatedSample.sourceSamples.size());
        for (int i = 0; i < expectedTimeStamps.length; ++i) {
            assertEquals(
                    "Timestamp of source sample at index " + i
                            + " does not match",
                    expectedTimeStamps[i], decimatedSample.sourceSamples.get(i)
                            .getRight().getTimeStamp());
        }
    }

    /**
     * Tests adding, updating, and removing a channel. This test verifies that
     * the {@link ArchiveConfigurationService} performs these operations
     * correctly and that the {@link ArchivingService} creates the corresponding
     * {@link ControlSystemChannel} after a channel has been added has been
     * added, destroys and recreates it when the channel is updated, and finally
     * destroys it when the channel is removed.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testAddUpdateAndRemoveChannel() throws Exception {
        final String channelName = "addUpdateAndRemoveChannel";
        FutureUtils.getUnchecked(
                archiveConfigurationService.addChannel(SERVER_UUID, channelName,
                        "test", ImmutableSet.of(0), ImmutableMap.of(0, 0), true,
                        ImmutableMap.<String, String> of()));
        // We do not use getChannel(...) here because the cache might not be
        // ready yet and would throw an exception.
        ChannelInformation channelInformation = FutureUtils.getUnchecked(
                channelInformationCache.getChannelAsync(channelName, false));
        assertEquals(channelName, channelInformation.getChannelName());
        assertEquals("test", channelInformation.getControlSystemType());
        assertEquals(ImmutableSet.of(0),
                channelInformation.getDecimationLevels());
        assertEquals(SERVER_UUID, channelInformation.getServerId());
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (channelCreated.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(
                SERVER_UUID, channelName, null, null, Collections.singleton(30),
                null, null, null, null, null, null));
        // Updating the channel should have the consequence of updating the
        // channel information and destroying and recreating the control-system
        // channel. The channel information should be up-to-date immediately
        // because the update operation should only complete once the cache has
        // been updated.
        channelInformation = FutureUtils.getUnchecked(
                channelInformationCache.getChannelAsync(channelName, false));
        assertEquals(ImmutableSet.of(0, 30),
                channelInformation.getDecimationLevels());
        // Reinitializing the channel happens asynchronously, so we might have
        // to wait.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (channelDestroyed.containsKey(channelName)
                    && channelCreated.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        // If the channel is still in the initializing state, we wait for a
        // moment.
        deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            ChannelStatus status = archivingService
                    .getChannelStatus(channelName);
            if (status != null && !status.getState()
                    .equals(ChannelStatus.State.INITIALIZING)) {
                break;
            }
            Thread.sleep(50L);
        }
        // The channel should be in the disconnected state.
        assertEquals(ChannelStatus.State.DISCONNECTED,
                archivingService.getChannelStatus(channelName).getState());
        // Now we explicitly set the state to see whether the archiving service
        // returns the correct state.
        channelStatus.put(channelName, ControlSystemChannelStatus.connected());
        assertEquals(ChannelStatus.State.OK,
                archivingService.getChannelStatus(channelName).getState());
        // Finally, we delete the channel and check whether the control-system
        // channel is destroyed as expected.
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
        assertNull(FutureUtils.getUnchecked(
                channelInformationCache.getChannelAsync(channelName, false)));
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (channelDestroyed.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
    }

    /**
     * Tests that a special "disabled" sample is written when a channel is
     * disabled.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testDisabledChannel() throws Exception {
        final String channelName = "disabledChannel";
        FutureUtils.getUnchecked(
                archiveConfigurationService.addChannel(SERVER_UUID, channelName,
                        "test", ImmutableSet.of(0), ImmutableMap.of(0, 0),
                        false, ImmutableMap.<String, String> of()));
        // As the channel is disabled, it should not be started. However, a
        // "disabled" sample should be generated and written. This might take a
        // moment, so we have to wait.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (!FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            0, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().isEmpty()) {
                break;
            }
            Thread.sleep(50L);
        }
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(1, foundSamples.size());
        assertTrue(((TestSample) foundSamples.get(0)).disabled);
        // No control-system channel should have been created, so there should
        // be no corresponding entry in the map.
        assertFalse(channelCreated.containsKey(channelName));
        // Finally, we remove the channel so that we do not disturb any other
        // tests.
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
    }

    /**
     * Tests that generating decimated samples works as expected.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testSampleDecimation() throws Exception {
        final String channelName = "sampleDecimation";
        // We create a decimation level with a decimation period of 5 seconds.
        FutureUtils.getUnchecked(archiveConfigurationService.addChannel(
                SERVER_UUID, channelName, "test", ImmutableSet.of(0, 5), null,
                true, ImmutableMap.<String, String> of()));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        SampleListener<TestSample> sampleListener = sampleListeners
                .remove(channelName);
        TestControlSystemChannel channel = channels.remove(channelName);
        sampleListener.onSampleReceived(channel, new TestSample(10L), 100);
        sampleListener.onSampleReceived(channel, new TestSample(20L), 100);
        sampleListener.onSampleReceived(channel, new TestSample(500L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(1L * ONE_BILLION + 20L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(1L * ONE_BILLION + 500L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(2L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(2L * ONE_BILLION + 700L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(4L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(5L * ONE_BILLION + 100L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(8L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(9L * ONE_BILLION + 300L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(10L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(12L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(15L * ONE_BILLION + 100L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(17L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(19L * ONE_BILLION + 0L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(20L * ONE_BILLION + 100L), 100);
        // It can take a moment for the samples to be actually written and the
        // decimated samples to be generated, so we have to wait a bit. We
        // expect three decimated samples.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            5, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 3) {
                break;
            }
            Thread.sleep(50L);
        }
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 5,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(3, foundSamples.size());
        assertEquals(5L * ONE_BILLION, foundSamples.get(0).getTimeStamp());
        assertEquals(10L * ONE_BILLION, foundSamples.get(1).getTimeStamp());
        assertEquals(15L * ONE_BILLION, foundSamples.get(2).getTimeStamp());
        // We also want to check that the right samples are used for building
        // each decimated sample.
        verifySourceSampleTimeStamps(foundSamples.get(0), 4L * ONE_BILLION + 0L,
                5L * ONE_BILLION + 100L, 8L * ONE_BILLION + 0L,
                9L * ONE_BILLION + 300L);
        verifySourceSampleTimeStamps(foundSamples.get(1),
                10L * ONE_BILLION + 0L, 12L * ONE_BILLION + 0L);
        verifySourceSampleTimeStamps(foundSamples.get(2),
                12L * ONE_BILLION + 0L, 15L * ONE_BILLION + 100L,
                17L * ONE_BILLION + 0L, 19L * ONE_BILLION + 0L);
        // We update the channel configuration and add two other decimation
        // levels. We actually test four things with this:
        // 1) Generating samples continues after a restart.
        // 2) When adding a new decimation level, the source samples from the
        // database are used correctly.
        // 3) Decimated samples from a compatible finer decimation level are
        // used to generate the samples for a coarser decimation level.
        // 4) The case when two different decimation levels use the same source
        // decimation level is handled correctly.
        FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(null,
                channelName, null, ImmutableSet.of(0, 5, 7, 10), null, null,
                null, null, null, null, null));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        sampleListener = sampleListeners.remove(channelName);
        channel = channels.remove(channelName);
        sampleListener.onSampleReceived(channel,
                new TestSample(23L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(26L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(28L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(31L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(32L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(33L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(35L * ONE_BILLION), 100);
        // It can take a moment for the samples to be actually written and the
        // decimated samples to be generated, so we have to wait a bit. We
        // expect three decimated samples.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            5, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 6
                    && FutureUtils.getUnchecked(archiveAccessService.getSamples(
                            channelName, 7, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                            .all().size() >= 4
                    && FutureUtils.getUnchecked(archiveAccessService.getSamples(
                            channelName, 10, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                            .all().size() >= 2) {
                break;
            }
            Thread.sleep(50L);
        }
        // First, we check the decimation level with a decimation period of five
        // seconds.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 5,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(6, foundSamples.size());
        assertEquals(5L * ONE_BILLION, foundSamples.get(0).getTimeStamp());
        assertEquals(10L * ONE_BILLION, foundSamples.get(1).getTimeStamp());
        assertEquals(15L * ONE_BILLION, foundSamples.get(2).getTimeStamp());
        assertEquals(20L * ONE_BILLION, foundSamples.get(3).getTimeStamp());
        assertEquals(25L * ONE_BILLION, foundSamples.get(4).getTimeStamp());
        assertEquals(30L * ONE_BILLION, foundSamples.get(5).getTimeStamp());
        // We also want to check that the right samples are used for building
        // each decimated sample. We do not have to check the first three
        // samples because we already checked them earlier.
        verifySourceSampleTimeStamps(foundSamples.get(3),
                19L * ONE_BILLION + 0L, 20L * ONE_BILLION + 100L,
                23L * ONE_BILLION + 0L);
        verifySourceSampleTimeStamps(foundSamples.get(4),
                23L * ONE_BILLION + 0L, 26L * ONE_BILLION + 0L,
                28L * ONE_BILLION + 0L);
        verifySourceSampleTimeStamps(foundSamples.get(5),
                28L * ONE_BILLION + 0L, 31L * ONE_BILLION + 0L,
                32L * ONE_BILLION + 0L, 33L * ONE_BILLION + 0L);
        // Next, we check the decimation level with a decimation period of seven
        // seconds.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 7,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(4, foundSamples.size());
        assertEquals(7L * ONE_BILLION, foundSamples.get(0).getTimeStamp());
        assertEquals(14L * ONE_BILLION, foundSamples.get(1).getTimeStamp());
        assertEquals(21L * ONE_BILLION, foundSamples.get(2).getTimeStamp());
        assertEquals(28L * ONE_BILLION, foundSamples.get(3).getTimeStamp());
        // We also want to check that the right samples are used for building
        // each decimated sample.
        verifySourceSampleTimeStamps(foundSamples.get(0),
                5L * ONE_BILLION + 100L, 8L * ONE_BILLION + 0L,
                9L * ONE_BILLION + 300L, 10L * ONE_BILLION + 0L,
                12L * ONE_BILLION + 0L);
        verifySourceSampleTimeStamps(foundSamples.get(1),
                12L * ONE_BILLION + 0L, 15L * ONE_BILLION + 100L,
                17L * ONE_BILLION + 0L, 19L * ONE_BILLION + 0L,
                20L * ONE_BILLION + 100L);
        verifySourceSampleTimeStamps(foundSamples.get(2),
                20L * ONE_BILLION + 100L, 23L * ONE_BILLION + 0L,
                26L * ONE_BILLION + 0L);
        verifySourceSampleTimeStamps(foundSamples.get(3),
                28L * ONE_BILLION + 0L, 31L * ONE_BILLION + 0L,
                32L * ONE_BILLION + 0L, 33L * ONE_BILLION + 0L);
        // Last, we check the decimation level with a decimation period of ten
        // seconds. This decimation level should have been built with samples
        // from the five second decimation level.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 10,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(2, foundSamples.size());
        assertEquals(10L * ONE_BILLION, foundSamples.get(0).getTimeStamp());
        assertEquals(20L * ONE_BILLION, foundSamples.get(1).getTimeStamp());
        // We also want to check that the right samples are used for building
        // each decimated sample.
        verifySourceSampleTimeStamps(foundSamples.get(0), 10L * ONE_BILLION,
                15L * ONE_BILLION);
        verifySourceSampleTimeStamps(foundSamples.get(1), 20L * ONE_BILLION,
                25L * ONE_BILLION);
        // Finally, we remove the channel so that we do not disturb any other
        // test. Removing the channel should also remove all sample buckets
        // (this is checked by another test).
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
    }

    /**
     * Tests that generating decimated samples from a source decimation-level
     * that has a finite retention period works correctly.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testSampleDecimationWithLimitedSourceDecimationLevel()
            throws Exception {
        final String channelName = "sampleDecimationWithLimitedSourceDecimationLevel";
        // We first create the channel with an infinite retention of raw samples
        // and without decimated samples.
        FutureUtils.getUnchecked(
                archiveConfigurationService.addChannel(SERVER_UUID, channelName,
                        "test", ImmutableSet.of(0), null, true, null));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        // Now that the channel is created, we can write a few samples. In order
        // to keep things simple, we choose the sample size so that each sample
        // is written to a separate bucket.
        SampleListener<TestSample> sampleListener = sampleListeners
                .remove(channelName);
        TestControlSystemChannel channel = channels.remove(channelName);
        sampleListener.onSampleReceived(channel,
                new TestSample(1L * ONE_BILLION), 150000000);
        sampleListener.onSampleReceived(channel,
                new TestSample(3L * ONE_BILLION), 150000000);
        sampleListener.onSampleReceived(channel,
                new TestSample(5L * ONE_BILLION), 150000000);
        sampleListener.onSampleReceived(channel,
                new TestSample(8L * ONE_BILLION), 150000000);
        sampleListener.onSampleReceived(channel,
                new TestSample(10L * ONE_BILLION), 150000000);
        // We wait until all these samples have been written.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            0, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 5) {
                break;
            }
            Thread.sleep(50L);
        }
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(5, foundSamples.size());
        // We update the channel configuration and limit the retention period of
        // the raw samples to two seconds and add a decimation level with a
        // decimation period of five seconds and unlimited sample retention.
        FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(null,
                channelName, null, ImmutableSet.of(0, 5), null, null,
                ImmutableMap.of(0, 2, 5, 0), null, null, null, null));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        sampleListener = sampleListeners.remove(channelName);
        channel = channels.remove(channelName);
        // Now we write a few more raw samples. This should trigger the deletion
        // of old sample buckets (which might otherwise be delayed
        // significantly). However, the sample buckets should not be deleted
        // before the decimated samples have been generated.
        sampleListener.onSampleReceived(channel,
                new TestSample(12L * ONE_BILLION), 150000000);
        sampleListener.onSampleReceived(channel,
                new TestSample(16L * ONE_BILLION), 150000000);
        // We wait for the decimated samples to be generated. We expect two
        // decimated samples.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            5, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 2) {
                break;
            }
            Thread.sleep(50L);
        }
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 5,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(2, foundSamples.size());
        assertEquals(5L * ONE_BILLION, foundSamples.get(0).getTimeStamp());
        assertEquals(10L * ONE_BILLION, foundSamples.get(1).getTimeStamp());
        verifySourceSampleTimeStamps(foundSamples.get(0), 5L * ONE_BILLION,
                8L * ONE_BILLION);
        verifySourceSampleTimeStamps(foundSamples.get(1), 10L * ONE_BILLION,
                12L * ONE_BILLION);
        // We write one more raw sample. The removal of old sample buckets is
        // paused until the decimated samples have been generated. It is
        // rescheduled, but there is a long delay. By writing another sample, we
        // can force an earlier run.
        sampleListener.onSampleReceived(channel,
                new TestSample(17L * ONE_BILLION), 150000000);
        // All raw samples except for the three last ones should be removed, but
        // we might have to wait a bit.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            0, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() == 3) {
                break;
            }
            Thread.sleep(50L);
        }
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(3, foundSamples.size());
        assertEquals(12L * ONE_BILLION, foundSamples.get(0).getTimeStamp());
        assertEquals(16L * ONE_BILLION, foundSamples.get(1).getTimeStamp());
        assertEquals(17L * ONE_BILLION, foundSamples.get(2).getTimeStamp());
        // Finally, we remove the channel so that we do not disturb any other
        // test. Removing the channel should also remove all sample buckets
        // (this is checked by another test).
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
    }

    /**
     * Tests that generating decimated samples works as expected when there are
     * many source samples.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testSampleDecimationWithManySourceSamples() throws Exception {
        final String channelName = "sampleDecimationWithManySourceSamples";
        // We create a decimation level with a decimation period of 5 seconds.
        FutureUtils.getUnchecked(archiveConfigurationService.addChannel(
                SERVER_UUID, channelName, "test", ImmutableSet.of(0, 5), null,
                true, ImmutableMap.<String, String> of()));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        SampleListener<TestSample> sampleListener = sampleListeners
                .remove(channelName);
        TestControlSystemChannel channel = channels.remove(channelName);
        for (long i = 0L; i < 120000L; ++i) {
            sampleListener.onSampleReceived(channel,
                    new TestSample(100000000L * i), 100);
        }
        // It can take a moment for the samples to be actually written and the
        // decimated samples to be generated, so we have to wait a bit. We
        // expect 2398 decimated samples.
        deadline = System.currentTimeMillis() + 30000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            5, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 2398) {
                break;
            }
            Thread.sleep(50L);
        }
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 5,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(2398, foundSamples.size());
        for (int i = 0; i < 2398; ++i) {
            assertEquals(5L * ONE_BILLION * (i + 1),
                    foundSamples.get(i).getTimeStamp());
            // We also want to check that the right samples are used for
            // building each decimated sample.
            long[] expectedTimeStamps = new long[50];
            for (int j = 0; j < 50; ++j) {
                expectedTimeStamps[j] = 5L * ONE_BILLION * (i + 1)
                        + 100000000L * j;
            }
            verifySourceSampleTimeStamps(foundSamples.get(i),
                    expectedTimeStamps);
        }
        // We update the channel configuration and add an additional decimation
        // level. With this test, we verify that when adding a new decimation
        // level, the source samples from the database are used correctly.
        FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(null,
                channelName, null, ImmutableSet.of(0, 2, 5), null, null, null,
                null, null, null, null));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        sampleListener = sampleListeners.remove(channelName);
        channel = channels.remove(channelName);
        // It can take a moment for the decimated samples to be generated, so we
        // have to wait a bit. We expect 5998 decimated samples.
        deadline = System.currentTimeMillis() + 30000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            2, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 5998) {
                break;
            }
            Thread.sleep(50L);
        }
        // Now we check the decimation level with a decimation period of two
        // seconds.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 2,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(5998, foundSamples.size());
        for (int i = 0; i < 5998; ++i) {
            assertEquals(2L * ONE_BILLION * (i + 1),
                    foundSamples.get(i).getTimeStamp());
            // We also want to check that the right samples are used for
            // building each decimated sample.
            long[] expectedTimeStamps = new long[20];
            for (int j = 0; j < 20; ++j) {
                expectedTimeStamps[j] = 2L * ONE_BILLION * (i + 1)
                        + 100000000L * j;
            }
            verifySourceSampleTimeStamps(foundSamples.get(i),
                    expectedTimeStamps);
        }
        // We update the channel configuration and removing the existing
        // decimation levels and adding a new one with a decimation period of
        // 300 seconds. This decimation period is so large that certain issues
        // that only occur when a decimated sample is created from many source
        // samples are exposed.
        FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(null,
                channelName, null, ImmutableSet.of(0, 300), null, null, null,
                null, null, null, null));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        sampleListener = sampleListeners.remove(channelName);
        channel = channels.remove(channelName);
        // It can take a moment for the decimated samples to be generated, so we
        // have to wait a bit. We expect 38 decimated samples.
        deadline = System.currentTimeMillis() + 30000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            300, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 38) {
                break;
            }
            Thread.sleep(50L);
        }
        // Now we check the newly created decimation level.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 300,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(38, foundSamples.size());
        for (int i = 0; i < 38; ++i) {
            assertEquals(300L * ONE_BILLION * (i + 1),
                    foundSamples.get(i).getTimeStamp());
            // We also want to check that the right samples are used for
            // building each decimated sample.
            long[] expectedTimeStamps = new long[3000];
            for (int j = 0; j < 3000; ++j) {
                expectedTimeStamps[j] = 300L * ONE_BILLION * (i + 1)
                        + 100000000L * j;
            }
            verifySourceSampleTimeStamps(foundSamples.get(i),
                    expectedTimeStamps);
        }
        // Finally, we remove the channel so that we do not disturb any other
        // test. Removing the channel should also remove all sample buckets
        // (this is checked by another test).
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
    }

    /**
     * Tests that generating decimated samples works as expected when source
     * samples are much scarcer than the generated samples. In such a situation,
     * the code will take a different path to limit the memory consumption. We
     * cannot (easily) test that this path is taken, but we can ensure that this
     * path does not introduce any bugs.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testSampleDecimationWithScarceSourceSamples() throws Exception {
        final String channelName = "sampleDecimationWithScarceSourceSamples";
        // We create a decimation level with a decimation period of 5 seconds.
        FutureUtils.getUnchecked(archiveConfigurationService.addChannel(
                SERVER_UUID, channelName, "test", ImmutableSet.of(0, 5), null,
                true, ImmutableMap.<String, String> of()));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        SampleListener<TestSample> sampleListener = sampleListeners
                .remove(channelName);
        TestControlSystemChannel channel = channels.remove(channelName);
        sampleListener.onSampleReceived(channel, new TestSample(20L), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(15000L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(35000L * ONE_BILLION), 100);
        sampleListener.onSampleReceived(channel,
                new TestSample(40000L * ONE_BILLION), 100);
        // It can take a moment for the samples to be actually written and the
        // decimated samples to be generated, so we have to wait a bit. We
        // expect decimated samples with time stamps from 5 seconds to
        // 39,995 seconds.
        deadline = System.currentTimeMillis() + 30000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            5, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 7999) {
                break;
            }
            Thread.sleep(50L);
        }
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 5,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(7999, foundSamples.size());
        for (int i = 0; i < foundSamples.size(); ++i) {
            long expectedTimeStamp = (i + 1) * 5L * ONE_BILLION;
            assertEquals(expectedTimeStamp, foundSamples.get(i).getTimeStamp());
        }
        // We update the channel configuration and add another decimation level.
        // By doing this, we can test that the code also works correctly when
        // generating samples from data already stored in the database.
        FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(null,
                channelName, null, ImmutableSet.of(0, 5, 7), null, null, null,
                null, null, null, null));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        sampleListener = sampleListeners.remove(channelName);
        channel = channels.remove(channelName);
        // It can take a moment for the decimated samples to be generated, so we
        // have to wait a bit. We expect decimated samples with time stamps from
        // 7 to 39991.
        deadline = System.currentTimeMillis() + 30000L;
        while (System.currentTimeMillis() < deadline) {
            if (FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            7, 0L, TimeStampLimitMode.AT_OR_AFTER,
                            Long.MAX_VALUE, TimeStampLimitMode.AT_OR_BEFORE))
                    .all().size() >= 5713) {
                break;
            }
            Thread.sleep(50L);
        }
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 7,
                        0L, TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(5713, foundSamples.size());
        for (int i = 0; i < foundSamples.size(); ++i) {
            long expectedTimeStamp = (i + 1) * 7L * ONE_BILLION;
            assertEquals(expectedTimeStamp, foundSamples.get(i).getTimeStamp());
        }
        // Finally, we remove the channel so that we do not disturb any other
        // test. Removing the channel should also remove all sample buckets
        // (this is checked by another test).
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
    }

    /**
     * Tests that samples are deleted after the retention period of the
     * respective sample bucket has passed.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testSampleRetention() throws Exception {
        final String channelName = "sampleRetention";
        // We use retention period of 100 seconds for the raw samples.
        // Basically, the retention period does not matter because we can choose
        // the time stamps of our samples arbitrarily.
        FutureUtils.getUnchecked(
                archiveConfigurationService.addChannel(SERVER_UUID, channelName,
                        "test", ImmutableSet.of(0), ImmutableMap.of(0, 100),
                        true, ImmutableMap.<String, String> of()));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        // Now that the channel is created, we can write a few samples. We can
        // control the number of sample buckets created by the sample sizes. A
        // sample bucket should usually have around 100 MB, so a sample
        // significantly exceeding this size should get its own sample bucket.
        SampleListener<TestSample> sampleListener = sampleListeners
                .remove(channelName);
        TestControlSystemChannel channel = channels.remove(channelName);
        // We remember the number of sample buckets that existed before (most
        // likely zero, but there might be some leftover from other tests). This
        // way, we can easily detect whether the expected number of sample
        // buckets has been created.
        int initialNumberOfSampleBuckets = sampleBuckets.size();
        sampleListener.onSampleReceived(channel, new TestSample(10L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(20L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(50L),
                150000000);
        sampleListener.onSampleReceived(channel, new TestSample(100L),
                150000000);
        sampleListener.onSampleReceived(channel, new TestSample(150L),
                150000000);
        sampleListener.onSampleReceived(channel, new TestSample(200L), 50000);
        // It can take a moment for the samples to be actually written, so we
        // have to wait a bit. We expect five sample buckets.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (sampleBuckets.size() - initialNumberOfSampleBuckets >= 5) {
                break;
            }
            Thread.sleep(50L);
        }
        assertEquals(5, sampleBuckets.size() - initialNumberOfSampleBuckets);
        // We check that the first three samples are actually the expected ones.
        // We also check that we can read the samples. However, we do not
        // request all of them. The samples in the last sample bucket might not
        // have been written yet (we only checked that the bucket exists).
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        0L, TimeStampLimitMode.AT_OR_AFTER, 50L,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(3, foundSamples.size());
        assertEquals(10L, foundSamples.get(0).getTimeStamp());
        assertEquals(20L, foundSamples.get(1).getTimeStamp());
        assertEquals(50L, foundSamples.get(2).getTimeStamp());
        // Now we write a few samples so that the newest sample is more than 100
        // seconds newer than the end of the first sample bucket (which should
        // be at 49 nanoseconds).
        sampleListener.onSampleReceived(channel, new TestSample(300L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(100000000050L),
                50000);
        // The first sample bucket should now be deleted. As this happens
        // asynchronously, it might take a moment.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (sampleBuckets.size() - initialNumberOfSampleBuckets <= 4) {
                break;
            }
            Thread.sleep(50L);
        }
        assertEquals(4, sampleBuckets.size() - initialNumberOfSampleBuckets);
        // The samples from the first sample bucket should have vanished.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        0L, TimeStampLimitMode.AT_OR_AFTER, 50L,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(1, foundSamples.size());
        assertEquals(50L, foundSamples.get(0).getTimeStamp());
        // When we write more samples, the second sample bucket should also
        // vanish.
        sampleListener.onSampleReceived(channel, new TestSample(100000000100L),
                50000);
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (sampleBuckets.size() - initialNumberOfSampleBuckets <= 3) {
                break;
            }
            Thread.sleep(50L);
        }
        assertEquals(3, sampleBuckets.size() - initialNumberOfSampleBuckets);
        // Finally, we remove the channel so that we do not disturb any other
        // test. Removing the channel should also remove all sample buckets
        // (this is checked by another test).
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
    }

    /**
     * Tests writing samples and reading them after they have been written. This
     * test adds a channel using the {@link ArchiveConfigurationService} and
     * writes a few samples after the corresponding {@link ControlSystemChannel}
     * has been created. Subsequently, it verifies that the samples have been
     * written and that sample buckets have been created when the accumulated
     * size of the written samples exceeded the limit. It also verifies that the
     * {@link ArchiveAccessService} returns the written samples when being
     * queried. Finally, the test removes the channel and verifies that the
     * corresponding sample buckets have been deleted.
     * 
     * @throws Exception
     *             if there is an error while running the test.
     */
    @Test
    public void testWriteAndReadSamples() throws Exception {
        final String channelName = "writeAndReadSamples";
        FutureUtils.getUnchecked(
                archiveConfigurationService.addChannel(SERVER_UUID, channelName,
                        "test", ImmutableSet.of(0), ImmutableMap.of(0, 0), true,
                        ImmutableMap.<String, String> of()));
        // The channel should be started. This means that we should find an
        // entry in the channelCreated map. However, it can be a moment before
        // this happens.
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertFalse(channelDestroyed.containsKey(channelName));
        // Now that the channel is created, we can write a few samples. We write
        // two extraordinarily large samples to check that they are actually
        // written to separate buckets. This is an implementation detail, but we
        // still test for it because it will really hurt the performance if this
        // is not done correctly.
        SampleListener<TestSample> sampleListener = sampleListeners
                .remove(channelName);
        TestControlSystemChannel channel = channels.remove(channelName);
        // We remember the number of sample buckets that existed before (most
        // likely zero, but there might be some leftover from other tests). This
        // way, we can easily detect whether the expected number of sample
        // buckets has been created.
        int initialNumberOfSampleBuckets = sampleBuckets.size();
        sampleListener.onSampleReceived(channel, new TestSample(123L),
                150000000);
        sampleListener.onSampleReceived(channel, new TestSample(456L),
                150000000);
        sampleListener.onSampleReceived(channel, new TestSample(457L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(458L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(500L),
                150000000);
        sampleListener.onSampleReceived(channel, new TestSample(510L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(515L), 50000);
        // It can take a moment for the samples to be actually written, so we
        // have to wait a bit. We expect five sample buckets.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (sampleBuckets.size() - initialNumberOfSampleBuckets >= 5) {
                break;
            }
            Thread.sleep(50L);
        }
        // We access the meta-data directly in order to check that the right
        // sample buckets have been created.
        List<SampleBucketInformation> foundSampleBuckets = FutureUtils
                .getUnchecked(
                        channelMetaDataDAO.getSampleBuckets(channelName, 0))
                .all();
        assertEquals(5, foundSampleBuckets.size());
        assertTrue(foundSampleBuckets.get(0).getBucketStartTime() <= 123L);
        assertTrue(foundSampleBuckets.get(0).getBucketEndTime() >= 123L);
        assertTrue(foundSampleBuckets.get(0)
                .getBucketEndTime() < foundSampleBuckets.get(1)
                        .getBucketStartTime());
        assertTrue(foundSampleBuckets.get(1).getBucketStartTime() <= 456L);
        assertTrue(foundSampleBuckets.get(1).getBucketEndTime() >= 456L);
        assertTrue(foundSampleBuckets.get(1)
                .getBucketEndTime() < foundSampleBuckets.get(2)
                        .getBucketStartTime());
        assertTrue(foundSampleBuckets.get(2).getBucketStartTime() <= 457L);
        assertTrue(foundSampleBuckets.get(2).getBucketEndTime() >= 458L);
        assertTrue(foundSampleBuckets.get(2)
                .getBucketEndTime() < foundSampleBuckets.get(3)
                        .getBucketStartTime());
        assertTrue(foundSampleBuckets.get(3).getBucketStartTime() <= 500L);
        assertTrue(foundSampleBuckets.get(3).getBucketEndTime() >= 500L);
        assertTrue(foundSampleBuckets.get(3)
                .getBucketEndTime() < foundSampleBuckets.get(4)
                        .getBucketStartTime());
        assertTrue(foundSampleBuckets.get(4).getBucketStartTime() <= 510L);
        assertTrue(foundSampleBuckets.get(4).getBucketEndTime() >= 515L);
        // We also check that we can read the samples. However, we do not
        // request all of them. The samples in the last sample bucket might not
        // have been written yet (we only checked that the bucket exists).
        List<? extends Sample> foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        0L, TimeStampLimitMode.AT_OR_AFTER, 500L,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(5, foundSamples.size());
        assertEquals(123L, foundSamples.get(0).getTimeStamp());
        assertEquals(456L, foundSamples.get(1).getTimeStamp());
        assertEquals(457L, foundSamples.get(2).getTimeStamp());
        assertEquals(458L, foundSamples.get(3).getTimeStamp());
        assertEquals(500L, foundSamples.get(4).getTimeStamp());
        // We want to refresh the channel and write a few more samples. This
        // verifies that the archiving service correctly finds the most recent
        // sample bucket when being started. As far as the archiving service is
        // concerned, refreshing a channel has exactly the same effect as
        // restarting the whole server.
        // Before refreshing the channel, we have to wait until the last two
        // samples have also been written because we need them for our next
        // test.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (!FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            0, 515L, TimeStampLimitMode.AT_OR_AFTER, 515L,
                            TimeStampLimitMode.AT_OR_BEFORE))
                    .isExhausted()) {
                break;
            }
        }
        assertFalse(FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        515L, TimeStampLimitMode.AT_OR_AFTER, 515L,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .isExhausted());
        FutureUtils.getUnchecked(archivingService.refreshChannel(channelName));
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // We have to wait until the sample listener is available which
            // might be a bit later than the created flag is set.
            if (channelCreated.containsKey(channelName)
                    && sampleListeners.containsKey(channelName)) {
                break;
            }
            Thread.sleep(50L);
        }
        assertTrue(channelCreated.containsKey(channelName));
        channelCreated.remove(channelName);
        assertTrue(channelDestroyed.containsKey(channelName));
        channelDestroyed.remove(channelName);
        channel = channels.remove(channelName);
        sampleListener = sampleListeners.remove(channelName);
        // We write one sample with a time-stamp that is less than the
        // time-stamp of the most recent sample. If the archiving service works
        // correctly, it should reject this sample.
        sampleListener.onSampleReceived(channel, new TestSample(514L), 50000);
        sampleListener.onSampleReceived(channel, new TestSample(520L), 50000);
        // We have to wait for the second sample to be actually written.
        deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (!FutureUtils
                    .getUnchecked(archiveAccessService.getSamples(channelName,
                            0, 520L, TimeStampLimitMode.AT_OR_AFTER, 520L,
                            TimeStampLimitMode.AT_OR_BEFORE))
                    .isExhausted()) {
                break;
            }
        }
        assertFalse(FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        520L, TimeStampLimitMode.AT_OR_AFTER, 520L,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .isExhausted());
        // The number of sample buckets should not have changed because the
        // sample should have been added to the existing bucket.
        foundSampleBuckets = FutureUtils
                .getUnchecked(
                        channelMetaDataDAO.getSampleBuckets(channelName, 0))
                .all();
        assertEquals(5, foundSampleBuckets.size());
        // The first sample should not have been written.
        foundSamples = FutureUtils
                .getUnchecked(archiveAccessService.getSamples(channelName, 0,
                        510L, TimeStampLimitMode.AT_OR_AFTER, 520L,
                        TimeStampLimitMode.AT_OR_BEFORE))
                .all();
        assertEquals(3, foundSamples.size());
        assertEquals(510L, foundSamples.get(0).getTimeStamp());
        assertEquals(515L, foundSamples.get(1).getTimeStamp());
        assertEquals(520L, foundSamples.get(2).getTimeStamp());
        // Remove the channel and test that the sample buckets are also removed.
        FutureUtils.getUnchecked(
                archiveConfigurationService.removeChannel(null, channelName));
        assertEquals(initialNumberOfSampleBuckets, sampleBuckets.size());
    }

}
