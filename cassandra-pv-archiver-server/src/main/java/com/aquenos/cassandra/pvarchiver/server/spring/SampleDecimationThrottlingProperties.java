/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Configuration properties that control throttling of the sample decimation
 * process. Throttling is needed in order to avoid exhausting system resources
 * or overloading the Cassandra database cluster. This object is injected with
 * properties that have the <code>throttling.sampleDecimation.*</code> prefix.
 * </p>
 * 
 * <p>
 * When adding a decimation level to a lot of channels that already have a lot
 * of raw samples, generating the decimated samples can consume a lot of
 * resources because this process involves reading all the source (raw) samples.
 * In particular, due to the fact that samples are read from the database in
 * pages, a lot of memory might be consumed for samples that have been read from
 * the database, but have not been processed yet. This can lead to a situation
 * in which the decimation process consumes all available heap memory and
 * triggers an <code>OutOfMemoryError</code>. By limiting the number of samples
 * that may be kept in memory and delaying the reading of samples for other
 * channels until some of the samples in memory have been processed, the amount
 * of memory used can be limited.
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
 * @see ThrottlingProperties
 */
public class SampleDecimationThrottlingProperties {

    private int maxFetchedSamplesInMemory = 1000000;
    private int maxRunningFetchOperations = 20;

    /**
     * Creates new sample decimation throttling properties initialized with the
     * respective default values.
     */
    public SampleDecimationThrottlingProperties() {
    }

    /**
     * Creates new sample decimation throttling properties initialized with the
     * respective default values. This constructor is only provided so that a
     * YAML file that has a "throttling.sampleDecimation:" line without any
     * sub-keys is parsed correctly.
     * 
     * @param str
     *            empty string or <code>null</code>.
     * @throws IllegalArgumentException
     *             if <code>str</code> is a non-empty string.
     */
    public SampleDecimationThrottlingProperties(String str) {
        this();
        if (str != null && !str.isEmpty()) {
            // The exception message might be irritating when calling this
            // constructor explicitly, but it will most likely be called when
            // parsing the configuration file and then this message gives an
            // accurate hint about the cause of the problem.
            throw new IllegalArgumentException(
                    "The \"sampleDecimation.throttlingProperties\" expects key-value pairs instead of a string.");
        }
    }

    /**
     * <p>
     * Returns the maximum number of samples that may be concurrently fetched
     * into memory when generating decimated samples. When this threshold is
     * exceeded, further fetch operations are postponed. These postponed fetch
     * operations are started when the number of samples in memory drops below
     * this threshold again.
     * </p>
     * 
     * <p>
     * As the exact number of samples returned by a fetch operation cannot be
     * known in advance, this threshold might actually be exceeded slightly. The
     * number returned by the {@link #getMaxRunningFetchOperations()} method
     * effectively controls by how much the threshold may be exceeded.
     * </p>
     * 
     * <p>
     * This option only controls how samples are fetched when generating
     * decimated samples. It does not affecting fetching samples for regular
     * archive access operations.
     * </p>
     * 
     * <p>
     * The default value is 1000000 samples.
     * </p>
     * 
     * @return max. number of samples that may be fetched into memory
     *         concurrently.
     */
    public int getMaxFetchedSamplesInMemory() {
        return maxFetchedSamplesInMemory;
    }

    /**
     * <p>
     * Sets the maximum number of samples that may be concurrently fetched into
     * memory when generating decimated samples. When this threshold is
     * exceeded, further fetch operations are postponed. These postponed fetch
     * operations are started when the number of samples in memory drops below
     * this threshold again.
     * </p>
     * 
     * <p>
     * As the exact number of samples returned by a fetch operation cannot be
     * known in advance, this threshold might actually be exceeded slightly. The
     * number set through the {@link #setMaxRunningFetchOperations(int)} method
     * effectively controls by how much the threshold may be exceeded.
     * </p>
     * 
     * <p>
     * This option only controls how samples are fetched when generating
     * decimated samples. It does not affecting fetching samples for regular
     * archive access operations.
     * </p>
     * 
     * <p>
     * The default value is 1000000 samples.
     * </p>
     * 
     * @param maxFetchedSamplesInMemory
     *            max. number of samples that may be fetched into memory
     *            concurrently. Must be greater than zero.
     * @throws IllegalArgumentException
     *             if <code>maxFetchedSamplesInMemory</code> is less than one.
     */
    public void setMaxFetchedSamplesInMemory(int maxFetchedSamplesInMemory) {
        Preconditions.checkArgument(maxRunningFetchOperations > 0,
                "The max. number of samples concurrently fetched into memory for sample decimation must be greater than zero.");
        this.maxFetchedSamplesInMemory = maxFetchedSamplesInMemory;
    }

    /**
     * <p>
     * Returns the maximum number of sample fetch operation that may run
     * concurrently when generating decimated samples. When this threshold is
     * exceeded, further fetch operations are postponed. These postponed fetch
     * operations are started when a previously running fetch operation
     * completes.
     * </p>
     * 
     * <p>
     * This options is needed to control by how much the threshold returned by
     * the {@link #getMaxFetchedSamplesInMemory()} method may be exceeded. As
     * the exact number of samples returned by a fetch operation cannot be known
     * in advance, the threshold for the number of samples in memory might be
     * exceeded by the fetch size multiplied by the number of fetch operations
     * that may run concurrently. For example, if the fetch size is 5000 and the
     * max. number of running fetch operations is 20, there might be up to
     * 100000 samples more in memory than specified by
     * {@link #getMaxFetchedSamplesInMemory()}.
     * </p>
     * 
     * <p>
     * This option only controls how samples are fetched when generating
     * decimated samples. It does not affecting fetching samples for regular
     * archive access operations.
     * </p>
     * 
     * <p>
     * The default value is 20 fetch operations.
     * </p>
     * 
     * @return max. number of sample fetch operations that may run concurrently.
     */
    public int getMaxRunningFetchOperations() {
        return maxRunningFetchOperations;
    }

    /**
     * <p>
     * Sets the maximum number of sample fetch operation that may run
     * concurrently when generating decimated samples. When this threshold is
     * exceeded, further fetch operations are postponed. These postponed fetch
     * operations are started when a previously running fetch operation
     * completes.
     * </p>
     * 
     * <p>
     * This options controls by how much the threshold set through the
     * {@link #setMaxFetchedSamplesInMemory(int)} method may be exceeded. As the
     * exact number of samples returned by a fetch operation cannot be known in
     * advance, the threshold for the number of samples in memory might be
     * exceeded by the fetch size multiplied by the number of fetch operations
     * that may run concurrently. For example, if the fetch size is 5000 and the
     * max. number of running fetch operations is 20, there might be up to
     * 100000 samples more in memory than specified through
     * {@link #setMaxFetchedSamplesInMemory(int)}.
     * </p>
     * 
     * <p>
     * This option only controls how samples are fetched when generating
     * decimated samples. It does not affecting fetching samples for regular
     * archive access operations.
     * </p>
     * 
     * <p>
     * The default value is 20 fetch operations.
     * </p>
     * 
     * @param maxRunningFetchOperations
     *            max. number of sample fetch operations that may run
     *            concurrently.
     * @throws IllegalArgumentException
     *             if <code>maxRunningFetchOperations</code> is less than one.
     */
    public void setMaxRunningFetchOperations(int maxRunningFetchOperations) {
        Preconditions.checkArgument(maxRunningFetchOperations > 0,
                "The max. number of running fetch operations for sample decimation must be greater than zero.");
        this.maxRunningFetchOperations = maxRunningFetchOperations;
    }

}
