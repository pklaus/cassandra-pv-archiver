/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

/**
 * <p>
 * Stateful utility that decimates samples. The sample decimator is responsible
 * for generating decimated samples when one or more decimation levels exist for
 * a channel. For each decimated sample that is going to be generated, a new
 * sample decimator is created by calling the control-system support's
 * {@link ControlSystemSupport#createSampleDecimator(String, java.util.Map, long, long)
 * createSampleDecimator} method.
 * </p>
 * 
 * <p>
 * Unlike most components provided by a control-system support, the sample
 * decimator does not have to be thread-safe. The archiver creates a new sample
 * decimator for each decimated sample that is supposed to be generated and only
 * uses this sample decimator from a single thread.
 * </p>
 * 
 * <p>
 * The methods provided by this interface must not block or perform any
 * long-running actions because they might be called from time-sensitive
 * threads.
 * </p>
 * 
 * <p>
 * The contract between the archiver and the control-system support specifies
 * that the sample decimator is used in the following way and order:
 * </p>
 * 
 * <ol>
 * <li>The sample decimator is created by calling
 * {@link ControlSystemSupport#createSampleDecimator(String, java.util.Map, long, long)}
 * .</li>
 * <li>The sample decimator's {@link #processSample(Sample)} method is called
 * with a sample that has a time stamp less than or equal to the start time of
 * the interval. This way, the sample decimator always knows the channel's state
 * right from the beginning of the interval.</li>
 * <li>The {@link #processSample(Sample)} method is called for each additional
 * sample that is present in the interval. The samples are passed in the order
 * of their time-stamps.</li>
 * <li>When the last sample in the interval (that has a time-stamp strictly less
 * than the interval's start time plus length) has been passed, the
 * {@link #buildDecimatedSample()} method is called.</li>
 * <li>Finally, the archiver calls the {@link #getDecimatedSample()} and
 * {@link #getDecimatedSampleEstimatedSize()} methods to get the generated
 * decimated sample and its estimated size. After that, the sample decimator is
 * not used any longer.</li>
 * </ol>
 * 
 * <p>
 * The samples passed to {@link #processSample(Sample)} may be raw samples (that
 * were received from the control-system support through a
 * {@link SampleListener}). However, they may also be decimated samples if the
 * channel has several decimation levels and the decimated sample for such a
 * level is supposed to be generated. In this case, the samples passed to this
 * sample decimator may be decimated samples from a decimation level with a
 * shorter decimation period. The archiver may only use samples from another
 * decimation level instead of raw samples when the decimation periods are
 * compatible, meaning that the decimation period of the decimation for which
 * this decimator is supposed to generate a sample is an integer multiple of the
 * decimation period of the decimation level of the samples that are passed.
 * </p>
 * 
 * <p>
 * The sample returned by the {@link #getDecimatedSample()} method must always
 * have a time stamp that is equal to the interval start-time that was specified
 * when the sample decimator was created. Returning a sample with a different
 * time-stamp is considered an error.
 * </p>
 * 
 * <p>
 * The actual implementation of how samples are decimated can vary significantly
 * between different control-system supports or even depend on the internal
 * sample type and thus on the channel or the channel's configuration. For
 * example, a simple implementation might always choose the sample right at the
 * beginning of the period, thus simply decimating the samples. A more complex
 * implementation might calculate the average of all samples and create a
 * decimated sample that represents this average, possibly carrying additional
 * information like minimum and maximum values or the standard deviation.
 * </p>
 * 
 * <p>
 * Implementations that use a different strategy for generating decimated
 * samples depending on the channel's configuration should consider that
 * typically, decimated samples are generated and stored over a prolonged
 * period. If the configuration is changed within this period, the strategy for
 * generating the decimated samples might also change, leading to a situation in
 * which some of the decimated samples have been generated using one algorithm
 * and other decimated samples have been generated using a different algorithm.
 * It is completely legal for a control-system support to exhibit such behavior,
 * but in this case it also has to be able to deal with such data when
 * processing samples.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 * @param <SampleType>
 *            type that is implemented by all samples that are passed to this
 *            sample decimator and that is also implemented by the sample
 *            generated by this sample decimator. Typically, this is the sample
 *            type used by the control-system support that provides the
 *            implementation of this interface.
 */
public interface SampleDecimator<SampleType extends Sample> {

    /**
     * Builds the decimated sample. This method must only be called after
     * {@link #processSample(Sample)} has been called at least once. It must be
     * called before calling {@link #getDecimatedSample()} or
     * {@link #getDecimatedSampleEstimatedSize()}.
     * {@link #processSample(Sample)} must not be called again after calling
     * this method and this method must only be called once. This method may
     * throw an {@link IllegalStateException} if the specified call order is
     * violated. However, there is no guarantee that such an exception is
     * thrown.
     */
    void buildDecimatedSample();

    /**
     * Returns the name of the channel for which this sample decimator decimates
     * samples. The returned channel name is the name that was specified when
     * this sample decimator was created.
     * 
     * @return name identifying the channel covered by this sample decimator.
     */
    String getChannelName();

    /**
     * Returns the decimated sample that has been generated by
     * {@link #buildDecimatedSample()}. This sample has the time stamp of the
     * interval start as specified by {@link #getIntervalStartTime()}. This
     * method must only be called after calling {@link #buildDecimatedSample()}.
     * This method may throw an {@link IllegalStateException} if the specified
     * call order is violated. However, there is no guarantee that such an
     * exception is thrown.
     * 
     * @return sample that has been generated from the samples passed to
     *         {@link #processSample(Sample)}.
     */
    SampleType getDecimatedSample();

    /**
     * <p>
     * Returns the estimated size of the decimated sample. This is the size that
     * the sample is expected to have when it is serialized into the database.
     * The size is specified in bytes.
     * </p>
     * 
     * <p>
     * This information is used by the archiving code to keep track of the total
     * size of the samples that are stored inside a bucket. As storing too much
     * (or too little) data in a single bucket has an impact on performance, the
     * archiving code will decide to start a new bucket when a certain size has
     * been reached. Therefore, this estimate should be as accurate as possible.
     * </p>
     * 
     * <p>
     * This method must only be called after calling
     * {@link #buildDecimatedSample()}. This method may throw an
     * {@link IllegalStateException} if the specified call order is violated.
     * However, there is no guarantee that such an exception is thrown.
     * </p>
     * 
     * @return estimated size of the decimated sample (returned by
     *         {@link #getDecimatedSample()}) in bytes.
     */
    int getDecimatedSampleEstimatedSize();

    /**
     * <p>
     * Returns the length of the period for which this sample decimator
     * generates a decimated sample. The length is specified in nanoseconds. The
     * returned length is the length that was specified when this sample
     * decimator was created.
     * </p>
     * 
     * <p>
     * The period covered by this sample decimator is specified by the interval
     * [start time, start + length).
     * </p>
     * 
     * @return length of the period covered by this sample decimator (in
     *         nanosconds).
     */
    long getIntervalLength();

    /**
     * <p>
     * Returns the start time of the period for which this sample decimator
     * generated a decimated sample. The start time is specified as the number
     * of nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC). The
     * returned start time is the time that was specified when this sample
     * decimator was created.
     * </p>
     * 
     * <p>
     * The period covered by this sample decimator is specified by the interval
     * [start time, start + length).
     * </p>
     * 
     * @return start time of the period covered by this sample decimator.
     */
    long getIntervalStartTime();

    /**
     * <p>
     * Processes a sample, updating the internal state with the information from
     * the sample. This method must be called for every sample that is is in the
     * interval specified by the start time and length (as returned by
     * {@link #getIntervalStartTime()} and {@link #getIntervalLength()}).
     * </p>
     * 
     * <p>
     * The first sample passed to this method must have a time stamp that is
     * less than or equal to the interval start-time. This method must be called
     * at least once before calling {@link #buildDecimatedSample()}. It must not
     * be called after calling {@link #buildDecimatedSample()}. This method may
     * throw an {@link IllegalStateException} if the specified call order is
     * violated. However, there is no guarantee that such an exception is
     * thrown.
     * </p>
     * 
     * @param sample
     *            sample that shall be processed, updating the sample
     *            decimator's internal state.
     */
    void processSample(SampleType sample);

}
