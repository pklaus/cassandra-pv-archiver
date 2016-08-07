/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Service providing access to samples stored in the archive. This class
 * provides methods for reading samples from the archive without having to know
 * anything about the underlying storage model.
 * 
 * @author Sebastian Marsching
 */
public interface ArchiveAccessService {

    /**
     * <p>
     * Retrieves samples for the specified channel and decimation level. The
     * result set containing the samples matching the query is returned through
     * a future. If the query fails, the future throws an exception.
     * </p>
     * 
     * <p>
     * The process for retrieving samples may (and typically will) be iterative,
     * meaning that samples are not retrieved all at once but gradually as they
     * are read from the result set. For this reason, retrieving samples from
     * the result set can fail with an exception, even if the future completed
     * successfully.
     * </p>
     * 
     * <p>
     * The samples are returned in the natural order of their time-stamps
     * (ascending, older samples first).
     * </p>
     * 
     * <p>
     * Calls to this method do not block. Instead, this method returns a future
     * that completes when a longer running operation has completed in the
     * background.
     * </p>
     * 
     * <strong>Time-stamp limit mode</strong>
     * 
     * <p>
     * When specifying the same value for <code>lowerTimeStampLimit</code> and
     * <code>upperTimeStampLimit</code>, the number of samples returned might be
     * zero, one, or two, depending on which
     * <code>lowerTimeStampLimitMode</code> and
     * <code>upperTimeStampLimitMode</code> have been specified and whether a
     * sample exactly at the specified time-stamp exists.
     * </p>
     * 
     * <p>
     * If a sample exactly matching the specified time-stamp exists, only this
     * sample is returned, regardless of the specified limit modes.
     * </p>
     * 
     * <p>
     * If no such sample exists, but samples with a lesser and greater
     * time-stamp exist, the number of samples returned is specified by the
     * following table:
     * </p>
     * 
     * <table summary="number of samples returned for different combinations of time-stamp limit modes">
     * <tr>
     * <th>lowerTimeStampLimitMode</th>
     * <th>upperTimeStampLimitMode</th>
     * <th>number of samples</th>
     * </tr>
     * <tr>
     * <td>AT_OR_AFTER</td>
     * <td>AT_OR_BEFORE</td>
     * <td>0</td>
     * </tr>
     * <tr>
     * <td>AT_OR_AFTER</td>
     * <td>AT_OR_AFTER</td>
     * <td>1</td>
     * </tr>
     * <tr>
     * <td>AT_OR_BEFORE</td>
     * <td>AT_OR_BEFORE</td>
     * <td>1</td>
     * </tr>
     * <tr>
     * <td>AT_OR_BEFORE</td>
     * <td>AT_OR_AFTER</td>
     * <td>2</td>
     * </tr>
     * </table>
     * 
     * <p>
     * In other words, specifying <code>AT_OR_BEFORE</code> for the
     * <code>lowerTimeStampLimitMode</code> ensures that the
     * <code>lowerTimeStampLimit</code> is always included in the time interval
     * spanned by the returned samples. Consequently, specifying
     * <code>AT_OR_AFTER</code> for the <code>upperTimeStampLimitMode/</code>
     * ensures that the <code>upperTimeStampLimit</code> is always included in
     * the time interval spanned by the returned samples.
     * </p>
     * 
     * <p>
     * For obvious reasons, these expectations only hold if there are enough
     * samples to span the entire interval. If the
     * <code>lowerTimeStampLimit</code> is less than the time stamp of the
     * oldest existing sample, that sample will always be the first sample being
     * returned. Consequently, if the <code>upperTimeStampLimit</code> is
     * greater than the time stamp of the newest existing sample, that sample
     * will always be the last sample being returned.
     * </p>
     * 
     * <strong>Performance considerations</strong>
     * 
     * <p>
     * In order to get the best possible performance, <code>AT_OR_AFTER</code>
     * should be preferred for the <code>lowerTimeStampLimitMode</code> and
     * <code>AT_OR_BEFORE</code> should be preferred for the
     * <code>upperTimeStampLimitMode</code>. Specifying different limit modes
     * will result in additional queries to the database. However, using
     * different limit modes might be preferable if samples spanning the entire
     * specified period are needed.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which samples shall be
     *            retrieved.
     * @param decimationLevel
     *            decimation level (identified by its decimation period) for
     *            which samples shall be received. A decimation period of zero
     *            specifies that raw samples shall be retrieved.
     * @param lowerTimeStampLimit
     *            lower limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>lowerTimeStampLimitMode</code>. This parameter must not
     *            be negative.
     * @param lowerTimeStampLimitMode
     *            mode for the <code>lowerTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the
     *            first sample returned has a time-stamp equal to or less than
     *            the <code>lowerTimeStampLimit</code> (if such a sample exists
     *            at all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the first
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>lowerTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param upperTimeStampLimit
     *            upper limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>upperTimeStampLimitMode</code>. This parameter must be
     *            greater than or equal to the <code>lowerTimeStampLimit</code>.
     * @param upperTimeStampLimitMode
     *            mode for the <code>upperTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the last
     *            sample returned has a time-stamp equal to or less than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the last
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @return future returning a result set which iterates over the samples
     *         matching the specified criteria. If there is an error, the future
     *         may throw an exception or the sample set returned by the future
     *         may throw an exception.
     * @see #getSamples(ChannelMetaDataDAO.ChannelInformation, int, long,
     *      TimeStampLimitMode, long, TimeStampLimitMode, ControlSystemSupport)
     */
    ListenableFuture<? extends ObjectResultSet<? extends Sample>> getSamples(
            String channelName, int decimationLevel, long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit, TimeStampLimitMode upperTimeStampLimitMode);

    /**
     * <p>
     * Retrieves samples for the specified channel and decimation level. The
     * result set containing the samples matching the query is returned through
     * a future. If the query fails, the future throws an exception.
     * </p>
     * 
     * <p>
     * This method is practically identical to the
     * {@link #getSamples(ChannelMetaDataDAO.ChannelInformation, int, long, TimeStampLimitMode, long, TimeStampLimitMode, ControlSystemSupport)
     * getSamples} method that takes a <code>ChannelInformation</code> argument,
     * so all the comments regarding that method apply to this method as well.
     * The only difference is that this method takes
     * <code>ChannelConfiguration</code> argument, so that it can be used in
     * situations where the <code>ChannelConfiguration</code> is directly
     * available, but the <code>ChannelInformation</code> is not.
     * </p>
     * 
     * @param <SampleType>
     *            type of the sample objects from the specified
     *            <code>controlSystemSupport</code>.
     * @param channelConfiguration
     *            channel configuration for the channel for which samples shall
     *            be retrieved.
     * @param decimationLevel
     *            decimation level (identified by its decimation period) for
     *            which samples shall be received. A decimation period of zero
     *            specifies that raw samples shall be retrieved.
     * @param lowerTimeStampLimit
     *            lower limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>lowerTimeStampLimitMode</code>. This parameter must not
     *            be negative.
     * @param lowerTimeStampLimitMode
     *            mode for the <code>lowerTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the
     *            first sample returned has a time-stamp equal to or less than
     *            the <code>lowerTimeStampLimit</code> (if such a sample exists
     *            at all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the first
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>lowerTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param upperTimeStampLimit
     *            upper limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>upperTimeStampLimitMode</code>. This parameter must be
     *            greater than or equal to the <code>lowerTimeStampLimit</code>.
     * @param upperTimeStampLimitMode
     *            mode for the <code>upperTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the last
     *            sample returned has a time-stamp equal to or less than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the last
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param controlSystemSupport
     *            control-system support for the channel for which samples shall
     *            be retrieved. The control-system support's type must match the
     *            control-system type specified by the supplied
     *            <code>channelConfiguration</code>.
     * @return future returning a result set which iterates over the samples
     *         matching the specified criteria. If there is an error, the future
     *         may throw an exception or the sample set returned by the future
     *         may throw an exception.
     * @see #getSamples(String, int, long, TimeStampLimitMode, long,
     *      TimeStampLimitMode)
     */
    <SampleType extends Sample> ListenableFuture<ObjectResultSet<SampleType>> getSamples(
            ChannelConfiguration channelConfiguration, int decimationLevel,
            long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit,
            TimeStampLimitMode upperTimeStampLimitMode,
            ControlSystemSupport<SampleType> controlSystemSupport);

    /**
     * <p>
     * Retrieves samples for the specified channel and decimation level. The
     * result set containing the samples matching the query is returned through
     * a future. If the query fails, the future throws an exception.
     * </p>
     * 
     * <p>
     * The process for retrieving samples may (and typically will) be iterative,
     * meaning that samples are not retrieved all at once but gradually as they
     * are read from the result set. For this reason, retrieving samples from
     * the result set can fail with an exception, even if the future completed
     * successfully.
     * </p>
     * 
     * <p>
     * The samples are returned in the natural order of their time-stamps
     * (ascending, older samples first).
     * </p>
     * 
     * <p>
     * Calls to this method do not block. Instead, this method returns a future
     * that completes when a longer running operation has completed in the
     * background.
     * </p>
     * 
     * <strong>Time-stamp limit mode</strong>
     * 
     * <p>
     * When specifying the same value for <code>lowerTimeStampLimit</code> and
     * <code>upperTimeStampLimit</code>, the number of samples returned might be
     * zero, one, or two, depending on which
     * <code>lowerTimeStampLimitMode</code> and
     * <code>upperTimeStampLimitMode</code> have been specified and whether a
     * sample exactly at the specified time-stamp exists.
     * </p>
     * 
     * <p>
     * If a sample exactly matching the specified time-stamp exists, only this
     * sample is returned, regardless of the specified limit modes.
     * </p>
     * 
     * <p>
     * If no such sample exists, but samples with a lesser and greater
     * time-stamp exist, the number of samples returned is specified by the
     * following table:
     * </p>
     * 
     * <table summary="number of samples returned for different combinations of time-stamp limit modes">
     * <tr>
     * <th>lowerTimeStampLimitMode</th>
     * <th>upperTimeStampLimitMode</th>
     * <th>number of samples</th>
     * </tr>
     * <tr>
     * <td>AT_OR_AFTER</td>
     * <td>AT_OR_BEFORE</td>
     * <td>0</td>
     * </tr>
     * <tr>
     * <td>AT_OR_AFTER</td>
     * <td>AT_OR_AFTER</td>
     * <td>1</td>
     * </tr>
     * <tr>
     * <td>AT_OR_BEFORE</td>
     * <td>AT_OR_BEFORE</td>
     * <td>1</td>
     * </tr>
     * <tr>
     * <td>AT_OR_BEFORE</td>
     * <td>AT_OR_AFTER</td>
     * <td>2</td>
     * </tr>
     * </table>
     * 
     * <p>
     * In other words, specifying <code>AT_OR_BEFORE</code> for the
     * <code>lowerTimeStampLimitMode</code> ensures that the
     * <code>lowerTimeStampLimit</code> is always included in the time interval
     * spanned by the returned samples. Consequently, specifying
     * <code>AT_OR_AFTER</code> for the <code>upperTimeStampLimitMode/</code>
     * ensures that the <code>upperTimeStampLimit</code> is always included in
     * the time interval spanned by the returned samples.
     * </p>
     * 
     * <p>
     * For obvious reasons, these expectations only hold if there are enough
     * samples to span the entire interval. If the
     * <code>lowerTimeStampLimit</code> is less than the time stamp of the
     * oldest existing sample, that sample will always be the first sample being
     * returned. Consequently, if the <code>upperTimeStampLimit</code> is
     * greater than the time stamp of the newest existing sample, that sample
     * will always be the last sample being returned.
     * </p>
     * 
     * <strong>Performance considerations</strong>
     * 
     * <p>
     * In order to get the best possible performance, <code>AT_OR_AFTER</code>
     * should be preferred for the <code>lowerTimeStampLimitMode</code> and
     * <code>AT_OR_BEFORE</code> should be preferred for the
     * <code>upperTimeStampLimitMode</code>. Specifying different limit modes
     * will result in additional queries to the database. However, using
     * different limit modes might be preferable if samples spanning the entire
     * specified period are needed.
     * </p>
     * 
     * @param <SampleType>
     *            type of the sample objects from the specified
     *            <code>controlSystemSupport</code>.
     * @param channelInformation
     *            channel information for the channel for which samples shall be
     *            retrieved.
     * @param decimationLevel
     *            decimation level (identified by its decimation period) for
     *            which samples shall be received. A decimation period of zero
     *            specifies that raw samples shall be retrieved.
     * @param lowerTimeStampLimit
     *            lower limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>lowerTimeStampLimitMode</code>. This parameter must not
     *            be negative.
     * @param lowerTimeStampLimitMode
     *            mode for the <code>lowerTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the
     *            first sample returned has a time-stamp equal to or less than
     *            the <code>lowerTimeStampLimit</code> (if such a sample exists
     *            at all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the first
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>lowerTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param upperTimeStampLimit
     *            upper limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>upperTimeStampLimitMode</code>. This parameter must be
     *            greater than or equal to the <code>lowerTimeStampLimit</code>.
     * @param upperTimeStampLimitMode
     *            mode for the <code>upperTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the last
     *            sample returned has a time-stamp equal to or less than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the last
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param controlSystemSupport
     *            control-system support for the channel for which samples shall
     *            be retrieved. The control-system support's type must match the
     *            control-system type specified by the supplied
     *            <code>channelInformation</code>.
     * @return future returning a result set which iterates over the samples
     *         matching the specified criteria. If there is an error, the future
     *         may throw an exception or the sample set returned by the future
     *         may throw an exception.
     * @see #getSamples(String, int, long, TimeStampLimitMode, long,
     *      TimeStampLimitMode)
     */
    <SampleType extends Sample> ListenableFuture<ObjectResultSet<SampleType>> getSamples(
            ChannelInformation channelInformation, int decimationLevel,
            long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit,
            TimeStampLimitMode upperTimeStampLimitMode,
            ControlSystemSupport<SampleType> controlSystemSupport);

}
