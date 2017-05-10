/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.List;
import java.util.UUID;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;

import com.aquenos.cassandra.pvarchiver.server.archiving.internal.ArchivingServiceInternalImpl;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.cluster.ServerOnlineStatusEvent;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.spring.ServerProperties;
import com.aquenos.cassandra.pvarchiver.server.spring.ThrottlingProperties;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Service performing the actual archiving. This service gets the list of
 * channels registered with this server when the server goes online and starts
 * archiving for these channels. It stops archiving when the server goes
 * offline. The configuration of individual channels can be reloaded using the
 * {@link #refreshChannel(String)} method.
 * 
 * @author Sebastian Marsching
 */
@ManagedResource(description = "handles archiving operations")
public class ArchivingService implements DisposableBean, InitializingBean,
        SelfNaming, SmartInitializingSingleton {

    private ArchiveAccessService archiveAccessService;
    private CassandraProvider cassandraProvider;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ClusterManagementService clusterManagementService;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private ArchivingServiceInternalImpl internalImpl;
    private ServerProperties serverProperties;
    private ThrottlingProperties throttlingProperties;

    @Override
    public void afterPropertiesSet() throws Exception {
        Preconditions.checkState(internalImpl == null,
                "The afterPropertiesSet() method must only be called once.");
        // All references to the other beans have to be set before calling this
        // method.
        Preconditions.checkState(archiveAccessService != null);
        Preconditions.checkState(cassandraProvider != null);
        Preconditions.checkState(channelMetaDataDAO != null);
        Preconditions.checkState(clusterManagementService != null);
        Preconditions.checkState(controlSystemSupportRegistry != null);
        Preconditions.checkState(serverProperties != null);
        Preconditions.checkState(throttlingProperties != null);
        UUID thisServerId = serverProperties.getUuid();
        internalImpl = new ArchivingServiceInternalImpl(archiveAccessService,
                cassandraProvider, channelMetaDataDAO,
                controlSystemSupportRegistry, throttlingProperties,
                thisServerId);
    }

    @Override
    public void afterSingletonsInstantiated() {
        // We start the background tasks here instead of starting them in
        // afterPropertiesSet(). In general, this is better because in the case
        // of circular dependencies, other beans might not be fully initialized
        // yet when afterPropertiesSet() is called. In addition to that, this is
        // better regardings events distributed by the application context.
        internalImpl.startBackgroundTasks();
        if (clusterManagementService.isOnline()) {
            // If we are already online, we might not get an event, so we have
            // to trigger the switch to the online state now.
            internalImpl.switchOnline();
        }
    }

    @Override
    public void destroy() throws Exception {
        Preconditions.checkState(internalImpl != null,
                "The destroy() method must be called after calling afterPropertiesSet().");
        internalImpl.destroy();
    }

    @Override
    public ObjectName getObjectName() throws MalformedObjectNameException {
        return ObjectName.getInstance(
                "com.aquenos.cassandra.pvarchiver.server:type=ArchivingService");
    }

    /**
     * Returns the status of an archived channel. If the channel is not known by
     * this archiving service, <code>null</code> is returned. Throws an
     * {@link IllegalStateException} if the archiving service has not finished
     * initialization of its channel list yet.
     * 
     * @param channelName
     *            name of the channel to be queried.
     * @return status of the specified channel or <code>null</code> if the
     *         specified channel is not known by this archiving service.
     * @throws IllegalStateException
     *             if the archiving service has not initialized its list of
     *             channels yet.
     */
    public ChannelStatus getChannelStatus(String channelName) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        return internalImpl.getChannelStatus(channelName);
    }

    /**
     * Returns a list of the status for all channels known by this archiving
     * service. Throws an {@link IllegalStateException} if the archiving service
     * has not finished initialization of its channel list yet. The channels are
     * returned in the natural order of their names. The returned list is never
     * <code>null</code>, but it might be empty if no channels belong this
     * server.
     * 
     * @return status of all channels known by this archiving service.
     * @throws IllegalStateException
     *             if the archiving service has not initialized its list f
     *             channels yet.
     */
    public List<ChannelStatus> getChannelStatusForAllChannels() {
        return internalImpl.getChannelStatusForAllChannels();
    }

    /**
     * Returns the total number of samples that have been dropped. A sample is
     * dropped when new samples for a channel arrive more quickly than they can
     * be written and consequently samples remain in the queue until a timeout
     * is reached. The returned number represents the total number of samples
     * that have been dropped for any channel since this archiving service was
     * started. When this number grows constantly, this can be an indication
     * that the server (or the Cassandra cluster) is overloaded and the workload
     * should be reduced (possibly by adding more servers).
     * 
     * @return number of samples that have been dropped since this archiving
     *         service was started.
     */
    @ManagedAttribute(description = "number of samples that been dropped since the archiving service was started")
    public long getNumberOfSamplesDropped() {
        return internalImpl.getNumberOfSamplesDropped();
    }

    /**
     * Returns the total number of samples that have been written. A sample is
     * only counted as written when the databases acknowledges that the write
     * operation has been successful. Samples for all decimation levels and not
     * just raw samples contribute to this number. The returned number
     * represents the total number of samples that have been written since this
     * archiving service was started.
     * 
     * @return number of samples that have been written since this archiving
     *         service was started.
     */
    @ManagedAttribute(description = "number of samples that been writen since the archiving service was started")
    public long getNumberOfSamplesWritten() {
        return internalImpl.getNumberOfSamplesWritten();
    }

    /**
     * <p>
     * Returns the number of sample fetch operations that are running currently.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. In this case, the number of fetch
     * operations that may run concurrently is limited (to the number returned
     * by {@link #getSamplesDecimationMaxRunningFetchOperations()}).
     * </p>
     * 
     * @return number of sample fetch operations (to generate decimated samples)
     *         that are currently running.
     * @see #getSamplesDecimationCurrentSamplesInMemory()
     * @see #getSamplesDecimationMaxRunningFetchOperations()
     */
    @ManagedAttribute(description = "number of sample fetch operations (to generate decimated samples) that are currently running")
    public int getSamplesDecimationCurrentRunningFetchOperations() {
        return internalImpl.getSamplesDecimationCurrentRunningFetchOperations();
    }

    /**
     * <p>
     * Returns the number of samples that have been fetched (but not processed
     * yet). This is the number of samples that is currently kept in memory.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. For performance reasons, samples are
     * fetched in batches. These samples, that have been fetched, but not
     * processed yet, consume memory, so that no new fetch operations may be
     * started if a large number of samples has already been fetched, but not
     * processed yet. The actual limit is returned by
     * {@link #getSamplesDecimationMaxSamplesInMemory()}.
     * </p>
     * 
     * @return number of samples that have been fetched (to generate decimated
     *         samples), but not processed yet.
     * @see #getSamplesDecimationCurrentRunningFetchOperations()
     * @see #getSamplesDecimationMaxSamplesInMemory()
     */
    @ManagedAttribute(description = "current number of samples that have been fetched (to generate decimated samples), but not processed yet")
    public int getSamplesDecimationCurrentSamplesInMemory() {
        return internalImpl.getSamplesDecimationCurrentSamplesInMemory();
    }

    /**
     * <p>
     * Returns the max. number of sample fetch operations that may run
     * concurrently.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. In this case, the number of fetch
     * operations that may run concurrently is limited to the number returned by
     * this method.
     * </p>
     * 
     * @return max. number of sample fetch operations (to generate decimated
     *         samples) that may run concurrently.
     * @see #getSamplesDecimationCurrentRunningFetchOperations()
     * @see #getSamplesDecimationMaxSamplesInMemory()
     */
    @ManagedAttribute(description = "max. number of sample fetch operations (to generate decimated samples) that may run concurrently")
    public int getSamplesDecimationMaxRunningFetchOperations() {
        return internalImpl.getSamplesDecimationMaxRunningFetchOperations();
    }

    /**
     * <p>
     * Returns the max. number of samples that may concurrently be kept in
     * memory.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. For performance reasons, samples are
     * fetched in batches. These samples, that have been fetched, but not
     * processed yet, consume memory, so that no new fetch operations may be
     * started if a large number of samples has already been fetched, but not
     * processed yet. This method returns the number of samples at which this
     * limit is enforced and no more samples are fetched.
     * </p>
     * 
     * @return max. number of samples that may be fetched (to generate decimated
     *         samples) into memory concurrently.
     * @see #getSamplesDecimationCurrentSamplesInMemory()
     * @see #getSamplesDecimationMaxRunningFetchOperations()
     */
    @ManagedAttribute(description = "max. number of samples that may be fetched into memory")
    public int getSamplesDecimationMaxSamplesInMemory() {
        return internalImpl.getSamplesDecimationMaxSamplesInMemory();
    }

    /**
     * <p>
     * Notifies the archiving service that the server's online status changed.
     * Typically, this method is automatically called by the Spring container
     * when the {@link ClusterManagementService} publishes
     * {@link ServerOnlineStatusEvent}.
     * </p>
     * 
     * <p>
     * This method takes care of starting archiving when the server goes online
     * and stopping it when the server goes offline. These operations are
     * performed asynchronously so that the thread sending the notification does
     * not block.
     * </p>
     * 
     * @param event
     *            event specifying the server's new online status.
     */
    @EventListener
    public void onServerOnlineStatusEvent(ServerOnlineStatusEvent event) {
        // We only consider events from the cluster management service that we
        // use.
        if (event.getSource() != clusterManagementService) {
            return;
        }

        // Trigger the switch to the online of offline state.
        if (event.isOnline()) {
            internalImpl.switchOnline();
        } else {
            internalImpl.switchOffline();
        }
    }

    /**
     * <p>
     * Reloads the configuration of the specified channel. This method should be
     * called whenever the configuration of a channel belonging to this server
     * has been changed. It also needs to be called after registering a pending
     * operation for a channel, so that archiving for the channel can be
     * disabled.
     * </p>
     * 
     * <p>
     * If the server is currently offline, calling this method will typically
     * have no effect because there is no need to reload the channel
     * configuration. In this case, the future returned by this method completed
     * immediately.
     * </p>
     * 
     * <p>
     * The future returned by this method completes when the refresh operation
     * has finished. It should never throw an exception.
     * </p>
     * 
     * @param channelName
     *            name of the channel to be refreshed.
     * @return future that completes when the refresh operation has finished.
     * @throws NullPointerException
     *             if <code>channelName</code> is <code>null</code>.
     */
    public ListenableFuture<Void> refreshChannel(String channelName) {
        Preconditions.checkNotNull(channelName);
        return internalImpl.refreshChannel(channelName);
    }

    /**
     * Sets the archive access service that is used to read samples from the
     * database. When generating decimated samples, the archiving service might
     * have to read samples that have been written earlier. It uses the archive
     * access service for this purpose. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param archiveAccessService
     *            archive access service used for reading samples when
     *            generating decimated samples.
     */
    @Autowired
    public void setArchiveAccessService(
            ArchiveAccessService archiveAccessService) {
        this.archiveAccessService = archiveAccessService;
    }

    /**
     * Sets the Cassandra provider that provides access to the Apache Cassandra
     * database. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param cassandraProvider
     *            provider that provides a connection to the Apache Cassandra
     *            database.
     */
    @Autowired
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

    /**
     * Sets the DAO for reading and modifying meta-data related to channels. The
     * archiving services uses this DAO to read the configuration for archived
     * channels and create the meta-data for new sample buckets when needed.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param channelMetaDataDAO
     *            channel meta-data DAO to be used by this object.
     */
    @Autowired
    public void setChannelMetaDataDAO(ChannelMetaDataDAO channelMetaDataDAO) {
        this.channelMetaDataDAO = channelMetaDataDAO;
    }

    /**
     * Sets the cluster management service. The cluster management service is
     * used to get information about the cluster status. In particular, it is
     * needed to determine the online state of this server in order to now when
     * archiving should be started and stopped. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param clusterManagementService
     *            cluster manager used by the archiving server.
     */
    @Autowired
    public void setClusterManagementService(
            ClusterManagementService clusterManagementService) {
        this.clusterManagementService = clusterManagementService;
    }

    /**
     * Sets the control-system support registry. The registry is used to gain
     * access to the control-system supports available on this archiving server.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param controlSystemSupportRegistry
     *            control-system support registry for this archiving server.
     */
    @Autowired
    public void setControlSystemSupportRegistry(
            ControlSystemSupportRegistry controlSystemSupportRegistry) {
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
    }

    /**
     * Sets the configuration properties controlling throttling. These
     * configuration properties are used to limit resource usage by the sample
     * decimation process..
     * 
     * @param throttlingProperties
     *            configuration properties controlling throttling.
     */
    @Autowired
    public void setThrottlingProperties(
            ThrottlingProperties throttlingProperties) {
        this.throttlingProperties = throttlingProperties;
    }

    /**
     * Sets the server-specific configuration properties. These server
     * properties are used to get the ID of this archiving server.
     * 
     * @param serverProperties
     *            configuration properties used by this server.
     */
    @Autowired
    public void setServerProperties(ServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

}
