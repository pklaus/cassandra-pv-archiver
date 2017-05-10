/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Configuration properties that control throttling of operations. Throttling is
 * needed in order to avoid exhausting system resources or overloading the
 * Cassandra database cluster. This object is injected with properties that have
 * the <code>throttling.*</code> prefix.
 * </p>
 * 
 * <p>
 * At the moment, throttling options are provided for two areas of operation:
 * </p>
 * 
 * <p>
 * Concurrently executed Cassandra statements: Processing statements causes load
 * on the database cluster. When processing too many statements in parallel,
 * this can lead to a situation in which statements take so long to be processed
 * that they time out. This can be avoided by limiting the number of statements
 * that are executed concurrently. The limits can be set separately for two
 * areas: statements that read or write samples (such statements are used by the
 * control-system supports) and statements that deal with sample meta-data (such
 * statements are used by the <code>ChannelMetaDataDAO</code>.
 * </p>
 * 
 * <p>
 * Sample decimation: When adding a decimation level to a lot of channels that
 * already have a lot of raw samples, generating the decimated samples can
 * consume a lot of resources because this process involves reading all the
 * source (raw) samples. In particular, due to the fact that samples are read
 * from the database in pages, a lot of memory might be consumed for samples
 * that have been read from the database, but have not been processed yet. This
 * can lead to a situation in which the decimation process consumes all
 * available heap memory and triggers an <code>OutOfMemoryError</code>. By
 * limiting the number of samples that may be kept in memory and delaying the
 * reading of samples for other channels until some of the samples in memory
 * have been processed, the amount of memory used can be limited.
 * </p>
 * 
 * <p>
 * Instances of this class are safe for concurrent read access but are not safe
 * for concurrent write access. Typically, this should not be a problem because
 * an instance of this class is initialized once at application startup and then
 * only used for read access.
 * </p>
 * 
 * @author Sebastian Marsching
 */
@ConfigurationProperties(prefix = "throttling", ignoreUnknownFields = false)
public class ThrottlingProperties {

    private int maxConcurrentChannelMetaDataReadStatements;
    private int maxConcurrentChannelMetaDataWriteStatements;
    private int maxConcurrentControlSystemSupportReadStatements;
    private int maxConcurrentControlSystemSupportWriteStatements;
    @NestedConfigurationProperty
    private SampleDecimationThrottlingProperties sampleDecimation;

    /**
     * Creates a properties object, initializing all properties with their
     * default values.
     */
    public ThrottlingProperties() {
        this.maxConcurrentChannelMetaDataReadStatements = 64;
        this.maxConcurrentChannelMetaDataWriteStatements = 16;
        this.maxConcurrentControlSystemSupportReadStatements = 128;
        this.maxConcurrentControlSystemSupportWriteStatements = 512;
        this.sampleDecimation = new SampleDecimationThrottlingProperties();
    }

    /**
     * Returns the maximum number of read statements that may be concurrently
     * executed by the {@link ChannelMetaDataDAO}. Typically, these are read
     * statements that load a channel configuration or information object or
     * that look for pending channel operations. The default value is 64.
     * 
     * @return maximum number of concurrently running read statements allowed
     *         for the <code>ChannelMetaDataDAO</code>.
     */
    public int getMaxConcurrentChannelMetaDataReadStatements() {
        return maxConcurrentChannelMetaDataReadStatements;
    }

    /**
     * Sets the maximum number of read statements that may be concurrently
     * executed by the {@link ChannelMetaDataDAO}. Typically, these are read
     * statements that load a channel configuration or information object or
     * that look for pending channel operations. The default value is 64.
     * 
     * @param maxConcurrentChannelMetaDataReadStatements
     *            maximum number of concurrently running read statements allowed
     *            for the <code>ChannelMetaDataDAO</code>. Must be greater than
     *            zero.
     * @throws IllegalArgumentException
     *             if the specified value is less than one.
     */
    public void setMaxConcurrentChannelMetaDataReadStatements(
            int maxConcurrentChannelMetaDataReadStatements) {
        Preconditions
                .checkArgument(
                        maxConcurrentChannelMetaDataReadStatements > 0,
                        "The maxConcurrentChannelMetaDataReadStatements parameter must be greater than zero.");
        this.maxConcurrentChannelMetaDataReadStatements = maxConcurrentChannelMetaDataReadStatements;
    }

    /**
     * Returns the maximum number of write statements that may be concurrently
     * executed by the {@link ChannelMetaDataDAO}. Typically, these are write
     * statements that modify a channel configuration and information object or
     * that add or remove pending channel operations. These write statements
     * typically are logged batch statements or light-weight transactions, so
     * they are significantly more expensive than regular write statements. The
     * default value is 16.
     * 
     * @return maximum number of concurrently running write statements allowed
     *         for the <code>ChannelMetaDataDAO</code>.
     */
    public int getMaxConcurrentChannelMetaDataWriteStatements() {
        return maxConcurrentChannelMetaDataWriteStatements;
    }

    /**
     * Sets the maximum number of write statements that may be concurrently
     * executed by the {@link ChannelMetaDataDAO}. Typically, these are write
     * statements that modify a channel configuration and information object or
     * that add or remove pending channel operations. These write statements
     * typically are logged batch statements or light-weight transactions, so
     * they are significantly more expensive than regular write statements. The
     * default value is 16.
     * 
     * @param maxConcurrentChannelMetaDataWriteStatements
     *            maximum number of concurrently running write statements
     *            allowed for the <code>ChannelMetaDataDAO</code>. Must be
     *            greater than zero.
     * @throws IllegalArgumentException
     *             if the specified value is less than one.
     */
    public void setMaxConcurrentChannelMetaDataWriteStatements(
            int maxConcurrentChannelMetaDataWriteStatements) {
        Preconditions
                .checkArgument(
                        maxConcurrentChannelMetaDataWriteStatements > 0,
                        "The maxConcurrentChannelMetaDataWriteStatements parameter must be greater than zero.");
        this.maxConcurrentChannelMetaDataWriteStatements = maxConcurrentChannelMetaDataWriteStatements;
    }

    /**
     * Returns the maximum number of read statements that may be concurrently
     * executed by all {@link ControlSystemSupport}s. Typically, these are read
     * statements that read samples from a sample bucket. All
     * <code>ControlSystemSupport</code>s share the same session and thus all
     * their statements contribute towards reaching this limit. The default
     * value is 128.
     * 
     * @return maximum number of concurrently running read statements allowed
     *         for all <code>ControlSystemSupport</code>s.
     */
    public int getMaxConcurrentControlSystemSupportReadStatements() {
        return maxConcurrentControlSystemSupportReadStatements;
    }

    /**
     * Sets the maximum number of read statements that may be concurrently
     * executed by all {@link ControlSystemSupport}s. Typically, these are read
     * statements that read samples from a sample bucket. All
     * <code>ControlSystemSupport</code>s share the same session and thus all
     * their statements contribute towards reaching this limit. The default
     * value is 128.
     * 
     * @param maxConcurrentControlSystemSupportReadStatements
     *            maximum number of concurrently running read statements allowed
     *            for all <code>ControlSystemSupport</code>s.
     * @throws IllegalArgumentException
     *             if the specified value is less than one.
     */
    public void setMaxConcurrentControlSystemSupportReadStatements(
            int maxConcurrentControlSystemSupportReadStatements) {
        Preconditions
                .checkArgument(
                        maxConcurrentControlSystemSupportReadStatements > 0,
                        "The maxConcurrentControlSystemSupportReadStatements parameter must be greater than zero.");
        this.maxConcurrentControlSystemSupportReadStatements = maxConcurrentControlSystemSupportReadStatements;
    }

    /**
     * Returns the maximum number of write statements that may be concurrently
     * executed by all {@link ControlSystemSupport}s. Typically, these are write
     * statements that write a single sample to a sample bucket, which means
     * that this kind of statements is rather cheap. All
     * <code>ControlSystemSupport</code>s share the same session and thus all
     * their statements contribute towards reaching this limit. The default
     * value is 512.
     * 
     * @return maximum number of concurrently running write statements allowed
     *         for all <code>ControlSystemSupport</code>s.
     */
    public int getMaxConcurrentControlSystemSupportWriteStatements() {
        return maxConcurrentControlSystemSupportWriteStatements;
    }

    /**
     * Sets the maximum number of write statements that may be concurrently
     * executed by all {@link ControlSystemSupport}s. Typically, these are write
     * statements that write a single sample to a sample bucket, which means
     * that this kind of statements is rather cheap. All
     * <code>ControlSystemSupport</code>s share the same session and thus all
     * their statements contribute towards reaching this limit. The default
     * value is 512.
     * 
     * @param maxConcurrentControlSystemSupportWriteStatements
     *            maximum number of concurrently running write statements
     *            allowed for all <code>ControlSystemSupport</code>s.
     * @throws IllegalArgumentException
     *             if the specified value is less than one.
     */
    public void setMaxConcurrentControlSystemSupportWriteStatements(
            int maxConcurrentControlSystemSupportWriteStatements) {
        Preconditions
                .checkArgument(
                        maxConcurrentControlSystemSupportWriteStatements > 0,
                        "The maxConcurrentControlSystemSupportWriteStatements parameter must be greater than zero.");
        this.maxConcurrentControlSystemSupportWriteStatements = maxConcurrentControlSystemSupportWriteStatements;
    }

    /**
     * Returns the configuration properties that control the sample decimation
     * process.
     * 
     * @return configuration properties for throttling the sample decimation
     *         process.
     */
    public SampleDecimationThrottlingProperties getSampleDecimation() {
        return sampleDecimation;
    }

    
    /**
     * Sets the configuration properties that control the sample decimation
     * process.
     * 
     * @param sampleDecimation
     *            configuration properties for throttling the sample decimation
     *            process.
     */
    public void setSampleDecimation(
            SampleDecimationThrottlingProperties sampleDecimation) {
        Preconditions.checkNotNull(sampleDecimation,
                "The sample decimation throttling properties must not be null.");
        this.sampleDecimation = sampleDecimation;
    }

}
