/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Data-access object handling meta-data related to archived control-system
 * channels. This DAO is responsible for storing channels with their
 * configuration and information about sample buckets for these channels.
 * However, it is not responsible for storing the actual samples.
 * </p>
 * 
 * <p>
 * In general, the methods provided by this class are thread-safe but do not
 * guarantee atomicity. For example, adding and removing the same channel
 * concurrently has undefined results. This also applies if two otherwise
 * independent clients (on different computers) run the code concurrently. The
 * caller has to ensure that methods are only invoked concurrently if there is
 * no risk of such interference (e.g. because the actions affect different
 * channels).
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface ChannelMetaDataDAO {

    /**
     * Value object representing the general meta-data for a channel. The
     * meta-data includes the control-system type, the decimation levels defined
     * for the channel, and the ID of the server responsible for the channel.
     * 
     * @author Sebastian Marsching
     */
    public final class ChannelInformation {

        private UUID channelDataId;
        private String channelName;
        private String controlSystemType;
        private Set<Integer> decimationLevels;
        private UUID serverId;

        /**
         * Creates a channel information object.
         * 
         * @param channelDataId
         *            unique identifier associated with the data (samples) for
         *            each channel. This identifier must be different for each
         *            channel. It is used instead of the channel name when
         *            storing data, so that the channel can be renamed later
         *            without losing data.
         * @param channelName
         *            name of this channel. The name is a textual identifier
         *            that uniquely identifies the channel within the archive.
         * @param controlSystemType
         *            control-system type of this channel. The control-system
         *            type defines the control-system that provides new data for
         *            the channel.
         * @param decimationLevels
         *            set of decimation levels defined for this channel.
         *            Typically, a channel at least has raw data (decimation
         *            level zero) associated with it. A decimation level is
         *            identified by the number of seconds between two samples.
         *            All decimation levels except the zero decimation level,
         *            which stores raw data, have fixed periods between samples.
         * @param serverId
         *            ID of the server that is responsible for this channel.
         * @throws IllegalArgumentException
         *             if the <code>channelName</code> is the empty string and
         *             if the <code>decimationLevels</code> set is empty.
         * @throws NullPointerException
         *             if the <code>channelDataId</code>, the
         *             <code>channelName</code>, the controlSystemType, or the
         *             <code>decimationLevels</code> are <code>null</code> and
         *             if the <code>decimationLevels</code> set contains
         *             <code>null</code> elements.
         */
        @JsonCreator
        public ChannelInformation(
                @JsonProperty(value = "channelDataId", required = true) UUID channelDataId,
                @JsonProperty(value = "channelName", required = true) String channelName,
                @JsonProperty(value = "controlSystemType", required = true) String controlSystemType,
                @JsonProperty(value = "decimationLevels", required = true) Set<Integer> decimationLevels,
                @JsonProperty(value = "serverId", required = true) UUID serverId) {
            Preconditions.checkNotNull(channelDataId,
                    "The channelDataId must not be null.");
            Preconditions.checkNotNull(channelName,
                    "The channelName must not be null.");
            Preconditions.checkArgument(!channelName.isEmpty(),
                    "The channelName must not be empty.");
            Preconditions.checkNotNull(controlSystemType,
                    "The controlSystemType must not be null.");
            Preconditions.checkNotNull(decimationLevels,
                    "The decimationLevels set must not be null.");
            Preconditions.checkArgument(!decimationLevels.isEmpty(),
                    "The decimationLevels set must not be empty.");
            if (Iterables.any(decimationLevels, Predicates.isNull())) {
                throw new NullPointerException(
                        "The decimationLevels set must not contain null elements.");
            }
            Preconditions.checkNotNull(serverId,
                    "The serverId must not be null.");
            this.channelDataId = channelDataId;
            this.channelName = channelName;
            this.controlSystemType = controlSystemType;
            this.decimationLevels = decimationLevels;
            this.serverId = serverId;
        }

        /**
         * Returns the unique identifier that is associated with the data
         * (samples) for the channel. While a channel's name might change due to
         * renaming, the data ID will be permanent (until the channel is
         * deleted), so even after renaming a channel, its associated data can
         * still be found.
         * 
         * @return unique identifier associated with the channel's data.
         */
        public UUID getChannelDataId() {
            return channelDataId;
        }

        /**
         * Returns the name of this channel. The name is a textual identifier
         * that uniquely identifies the channel within the archive.
         * 
         * @return channel name.
         */
        public String getChannelName() {
            return channelName;
        }

        /**
         * Returns the control-system type of this channel. The control-system
         * type defines the control-system that provides new data for the
         * channel.
         * 
         * @return control-system type of the channel.
         */
        public String getControlSystemType() {
            return controlSystemType;
        }

        /**
         * Returns the set of decimation levels defined for this channel.
         * Typically, a channel at least has raw data (decimation level zero)
         * associated with it. A decimation level is identified by the number of
         * seconds between two samples. All decimation levels except the zero
         * decimation level, which stores raw data, have fixed periods between
         * samples.
         * 
         * @return set of decimation levels defined for the channel.
         */
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        public Set<Integer> getDecimationLevels() {
            return decimationLevels;
        }

        /**
         * Returns the ID of the server that is responsible for this channel.
         * 
         * @return ID of the server owning this channel.
         */
        public UUID getServerId() {
            return serverId;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(channelDataId)
                    .append(channelName).append(controlSystemType)
                    .append(decimationLevels).append(serverId).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ChannelInformation)) {
                return false;
            }
            ChannelInformation other = (ChannelInformation) obj;
            return new EqualsBuilder()
                    .append(this.channelDataId, other.channelDataId)
                    .append(this.channelName, other.channelName)
                    .append(this.controlSystemType, other.controlSystemType)
                    .append(this.decimationLevels, other.decimationLevels)
                    .append(this.serverId, other.serverId).isEquals();
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

    }

    /**
     * Value object representing the configuration of a channel. This is the
     * information that a server needs to start archiving for a channel.
     * 
     * @author Sebastian Marsching
     */
    public final class ChannelConfiguration {

        private UUID channelDataId;
        private String channelName;
        private String controlSystemType;
        private Map<Integer, Long> decimationLevelToCurrentBucketStartTime;
        private Map<Integer, Integer> decimationLevelToRetentionPeriod;
        private boolean enabled;
        private Map<String, String> options;
        private UUID serverId;

        /**
         * Creates a channel configuration object.
         * 
         * @param channelDataId
         *            unique identifier associated with the data (samples) for
         *            each channel. This identifier must be different for each
         *            channel. It is used instead of the channel name when
         *            storing data, so that the channel can be renamed later
         *            without losing data.
         * @param channelName
         *            name of this channel. The name is a textual identifier
         *            that uniquely identifies the channel within the archive.
         * @param controlSystemType
         *            control-system type of this channel. The control-system
         *            type defines the control-system that provides new data for
         *            the channel.
         * @param decimationLevelToCurrentBucketStartTime
         *            map that contains the decimation levels for the channel as
         *            keys and the start time of the corresponding most recent
         *            sample buckets as values. If a decimation level does not
         *            yet have a sample bucket, the respective value is
         *            <code>-1</code>.
         * @param decimationLevelToRetentionPeriod
         *            map that contains the retention periods of the decimation
         *            levels for the channel. The decimation levels are used as
         *            keys and the corresponding retention periods (in seconds)
         *            are stored as values. A retention period that is zero or
         *            negative means that samples for the corresponding
         *            decimation level are supposed to be retained forever.
         * @param enabled
         *            <code>true</code> if archiving is enabled and the server
         *            should archive new samples received for the channel.
         *            <code>false</code> if archiving is disabled and the server
         *            should not archive new samples for the channel but just
         *            use the samples that have been archived previously.
         * @param options
         *            map storing the configuration options for this channel.
         *            The configuration options are passed on to the
         *            control-system specific adapter. The meaning of the
         *            options depends on this control-system specific code. The
         *            map does not contain <code>null</code> keys or values.
         * @param serverId
         *            ID of the server that is responsible for this channel.
         * @throws IllegalArgumentException
         *             if the <code>channelName</code> is the empty string, if
         *             the <code>decimationLevelToCurrentBucketStartTime</code>
         *             map is empty, and if the
         *             <code>decimationLevelToRetentionPeriod</code> map is
         *             empty.
         * @throws NullPointerException
         *             if the <code>channelDataId</code>, the
         *             <code>channelName</code>, the
         *             <code>controlSystemType</code>, the
         *             <code>decimationLevelToCurrentBucketStartTime</code> map,
         *             or the <code>decimationLevelToRetentionPeriod</code> map,
         *             is <code>null</code> and if the
         *             <code>decimationLevelToCurrentBucketStartTime</code> map
         *             or the <code>decimationLevelToRetentionPeriod</code> map
         *             contains <code>null</code> keys or values.
         */
        @JsonCreator
        public ChannelConfiguration(
                @JsonProperty(value = "channelDataId", required = true) UUID channelDataId,
                @JsonProperty(value = "channelName", required = true) String channelName,
                @JsonProperty(value = "controlSystemType", required = true) String controlSystemType,
                @JsonProperty(value = "decimationLevelToCurrentBucketStartTime", required = true) Map<Integer, Long> decimationLevelToCurrentBucketStartTime,
                @JsonProperty(value = "decimationLevelToRetentionPeriod", required = true) Map<Integer, Integer> decimationLevelToRetentionPeriod,
                @JsonProperty(value = "enabled", required = true) boolean enabled,
                @JsonProperty(value = "options", required = true) Map<String, String> options,
                @JsonProperty(value = "serverId", required = true) UUID serverId) {
            Preconditions.checkNotNull(channelDataId,
                    "The channelDataId must not be null.");
            Preconditions.checkNotNull(channelName,
                    "The channelName must not be null.");
            Preconditions.checkArgument(!channelName.isEmpty(),
                    "The channelName must not be empty.");
            Preconditions.checkNotNull(controlSystemType,
                    "The controlSystemType must not be null.");
            Preconditions
                    .checkNotNull(decimationLevelToCurrentBucketStartTime,
                            "The decimationLevelToCurrentBucketStartTime map must not be null.");
            Preconditions
                    .checkArgument(
                            !decimationLevelToCurrentBucketStartTime.isEmpty(),
                            "The decimationLevelToCurrentBucketStartTime map must not be empty.");
            if (Iterables.any(decimationLevelToCurrentBucketStartTime.keySet(),
                    Predicates.isNull())
                    || Iterables.any(
                            decimationLevelToCurrentBucketStartTime.values(),
                            Predicates.isNull())) {
                throw new NullPointerException(
                        "The decimationLevelToCurrentBucketStartTime map must not contain null keys or values.");
            }
            Preconditions
                    .checkNotNull(decimationLevelToRetentionPeriod,
                            "The decimationLevelToRetentionPeriod map must not be null.");
            Preconditions
                    .checkArgument(!decimationLevelToRetentionPeriod.isEmpty(),
                            "The decimationLevelToRetentionPeriod map must not be empty.");
            if (Iterables.any(decimationLevelToRetentionPeriod.keySet(),
                    Predicates.isNull())
                    || Iterables.any(decimationLevelToRetentionPeriod.values(),
                            Predicates.isNull())) {
                throw new NullPointerException(
                        "The decimationLevelToRetentionPeriod map must not contain null keys or values.");
            }
            Preconditions.checkNotNull(options,
                    "The options map must not be null.");
            if (Iterables.any(options.keySet(), Predicates.isNull())
                    || Iterables.any(options.values(), Predicates.isNull())) {
                throw new NullPointerException(
                        "The options map must not contain null keys or values.");
            }
            Preconditions.checkNotNull(serverId,
                    "The serverId must not be null.");
            this.channelDataId = channelDataId;
            this.channelName = channelName;
            this.controlSystemType = controlSystemType;
            this.decimationLevelToCurrentBucketStartTime = decimationLevelToCurrentBucketStartTime;
            this.decimationLevelToRetentionPeriod = decimationLevelToRetentionPeriod;
            this.enabled = enabled;
            this.options = options;
            this.serverId = serverId;
        }

        /**
         * Returns the unique identifier that is associated with the data
         * (samples) for the channel. While a channel's name might change due to
         * renaming, the data ID will be permanent (until the channel is
         * deleted), so even after renaming a channel, its associated data can
         * still be found.
         * 
         * @return unique identifier associated with the channel's data.
         */
        public UUID getChannelDataId() {
            return channelDataId;
        }

        /**
         * Returns the name of this channel. The name is a textual identifier
         * that uniquely identifies the channel within the archive.
         * 
         * @return channel name.
         */
        public String getChannelName() {
            return channelName;
        }

        /**
         * Returns the control-system type of this channel. The control-system
         * type defines the control-system that provides new data for the
         * channel.
         * 
         * @return control-system type of the channel.
         */
        public String getControlSystemType() {
            return controlSystemType;
        }

        /**
         * Returns a map that contains the decimation levels for the channel as
         * keys and the start time of the corresponding most recent sample
         * buckets as values. If a decimation level does not yet have a sample
         * bucket, the respective value is <code>-1</code>.
         * 
         * @return map mapping decimation levels to the start time of their
         *         current buckets.
         */
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        public Map<Integer, Long> getDecimationLevelToCurrentBucketStartTime() {
            return decimationLevelToCurrentBucketStartTime;
        }

        /**
         * Returns a map that contains the retention periods of the decimation
         * levels for the channel. The decimation levels are as used keys and
         * the corresponding retention periods (in seconds) are stored as
         * values. A retention period that is zero or negative means that
         * samples for the corresponding decimation level are supposed to be
         * retained forever.
         * 
         * @return map mapping decimation levels to their retention periods.
         */
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        public Map<Integer, Integer> getDecimationLevelToRetentionPeriod() {
            return decimationLevelToRetentionPeriod;
        }

        /**
         * Tells whether archiving is enabled for this channel. If
         * <code>true</code>, archiving is enabled and the server should archive
         * new samples received for the channel. If <code>false</code>,
         * archiving is disabled and the server should not archive new samples
         * for the channel but just use the samples that have been archived
         * previously.
         * 
         * @return <code>true</code> if archiving is enabled for this channel,
         *         <code>false</code> otherwise.
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Returns a map storing the configuration options for this channel. The
         * configuration options are passed on to the control-system specific
         * adapter. The meaning of the options depends on this control-system
         * specific code. The map does not contain <code>null</code> keys or
         * values.
         * 
         * @return map storing the configuration options for this channel.
         */
        public Map<String, String> getOptions() {
            return options;
        }

        /**
         * Returns the ID of the server that is responsible for this channel.
         * 
         * @return ID of the server owning this channel.
         */
        public UUID getServerId() {
            return serverId;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(channelDataId)
                    .append(channelName).append(controlSystemType)
                    .append(decimationLevelToCurrentBucketStartTime)
                    .append(decimationLevelToRetentionPeriod).append(enabled)
                    .append(options).append(serverId).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ChannelConfiguration)) {
                return false;
            }
            ChannelConfiguration other = (ChannelConfiguration) obj;
            return new EqualsBuilder()
                    .append(this.channelDataId, other.channelDataId)
                    .append(this.channelName, other.channelName)
                    .append(this.controlSystemType, other.controlSystemType)
                    .append(this.decimationLevelToCurrentBucketStartTime,
                            other.decimationLevelToCurrentBucketStartTime)
                    .append(this.decimationLevelToRetentionPeriod,
                            other.decimationLevelToRetentionPeriod)
                    .append(this.enabled, other.enabled)
                    .append(this.options, other.options)
                    .append(this.serverId, other.serverId).isEquals();
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

    }

    /**
     * Value object representing an operation on a channel. Each operation is
     * identified by a unique ID. The meaning of different operation types and
     * the associated data is left to the code using the object.
     * 
     * @author Sebastian Marsching
     */
    public final class ChannelOperation {

        private String channelName;
        private String operationData;
        private UUID operationId;
        private String operationType;
        private UUID serverId;

        /**
         * Creates a channel operation object.
         * 
         * @param channelName
         *            name of the channel that is affected by this operation.
         *            The name is a textual identifier that uniquely identifies
         *            the channel within the archive.
         * @param operationData
         *            data associated with this operation. The interpretation of
         *            the data depends on the <code>operationType</code>. May be
         *            <code>null</code>.
         * @param operationId
         *            unique ID identifying this operation. The ID is mainly
         *            used to allow updates or deletes of the operation in a way
         *            that ensures that no other operation is modified
         *            accidentally.
         * @param operationType
         *            textual identifier defining the type of this operation.
         *            The operation type also defines which kind of data is
         *            expected for <code>operationData</code> (if any). The
         *            definition of types is left to the code using this object.
         * @param serverId
         *            ID of the server that is affected by this operation. Most
         *            likely, this is the server that is responsible for the
         *            channel, but for some operations, a different server might
         *            be involved.
         * @throws IllegalArgumentException
         *             if the <code>channelName</code> is the empty string.
         * @throws NullPointerException
         *             if the <code>channelName</code>, the
         *             <code>operationId</code>, the <code>operationType</code>,
         *             or the <code>serverId</code> is <code>null</code>.
         */
        public ChannelOperation(String channelName, String operationData,
                UUID operationId, String operationType, UUID serverId) {
            Preconditions.checkNotNull(channelName,
                    "The channelName must not be null.");
            Preconditions.checkArgument(!channelName.isEmpty(),
                    "The channelName must not be empty.");
            Preconditions.checkNotNull(operationId,
                    "The operationId must not be null.");
            Preconditions.checkNotNull(operationType,
                    "The operationType must not be null.");
            Preconditions.checkNotNull(serverId,
                    "The serverId must not be null.");
            this.channelName = channelName;
            this.operationData = operationData;
            this.operationId = operationId;
            this.operationType = operationType;
            this.serverId = serverId;
        }

        /**
         * Returns the name of the channel that is affected by this operation.
         * The name is a textual identifier that uniquely identifies the channel
         * within the archive.
         * 
         * @return name of the channel affected by this operation.
         */
        public String getChannelName() {
            return channelName;
        }

        /**
         * Returns the data associated with this operation. The interpretation
         * of the data depends on the type of the operation (see
         * {@link #getOperationType()}). May be <code>null</code>.
         * 
         * @return data associated with this operation or <code>null</code> if
         *         no data has been stored for this operation.
         */
        public String getOperationData() {
            return operationData;
        }

        /**
         * Returns the unique ID identifying this operation. The ID is mainly
         * used to allow updates or deletes of the operation in a way that
         * ensures that no other operation is modified accidentally.
         * 
         * @return unique ID identifying this operation.
         */
        public UUID getOperationId() {
            return operationId;
        }

        /**
         * Returns the textual identifier defining the type of this operation.
         * The operation type also defines which kind of data is expected for
         * the data returned by {@link #getOperationData()} (if any). The
         * definition of types is left to the code using this object.
         * 
         * @return type of this operation.
         */
        public String getOperationType() {
            return operationType;
        }

        /**
         * Returns the ID of the server that is affected by this operation. Most
         * likely, this is the server that is responsible for the channel, but
         * for some operations, a different server might be involved.
         * 
         * @return ID of the server that is affected by this operation.
         */
        public UUID getServerId() {
            return serverId;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(channelName)
                    .append(operationData).append(operationId)
                    .append(operationType).append(serverId).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ChannelOperation)) {
                return false;
            }
            ChannelOperation other = (ChannelOperation) obj;
            return new EqualsBuilder()
                    .append(this.channelName, other.channelName)
                    .append(this.operationData, other.operationData)
                    .append(this.operationId, other.operationId)
                    .append(this.operationType, other.operationType)
                    .append(this.serverId, other.serverId).isEquals();
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

    }

    /**
     * Value object representing the meta-data of a sample bucket. Each sample
     * bucket belongs to a certain channel and decimation level and is
     * identified by a unique ID. It stores samples with a time stamp that is
     * equal to or greater than the bucket's start time.
     * 
     * @author Sebastian Marsching
     */
    public final class SampleBucketInformation {

        private long bucketEndTime;
        private SampleBucketId bucketId;
        private String channelName;

        /**
         * Creates a sample-bucket meta-data object.
         * 
         * @param bucketEndTime
         *            end time of this sample bucket as the number of
         *            nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
         * @param bucketStartTime
         *            start time of this sample bucket as the number of
         *            nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
         * @param channelDataId
         *            unique identifier associated with the data (samples) for
         *            the channel.
         * @param channelName
         *            name of the channel for which this sample bucket stores
         *            data.
         * @param decimationLevel
         *            decimation level for which this sample bucket stores data.
         *            The decimation level is identified by the number of
         *            seconds between two samples. A decimation level of zero
         *            specifies that this decimation level stores (undecimated)
         *            raw samples, which typically do not have a fixed period.
         * @throws IllegalArgumentException
         *             if the <code>channelName</code> is the empty string, if
         *             the <code>bucketStartTime</code> is negative, and if the
         *             <code>bucketEndTime</code> is less than the
         *             <code>bucketStartTime</code>.
         * @throws NullPointerException
         *             if the <code>channelDataId</code> or the
         *             <code>channelName</code> is <code>null</code>.
         */
        public SampleBucketInformation(long bucketEndTime,
                long bucketStartTime, UUID channelDataId, String channelName,
                int decimationLevel) {
            Preconditions.checkNotNull(channelName,
                    "The channelName must not be null.");
            Preconditions
                    .checkArgument(bucketEndTime >= bucketStartTime,
                            "The bucket end time cannot be less than the bucket start time.");
            // The SampleBucketId constructor validates its arguments, so we do
            // not have to check the other arguments ourselves.
            this.bucketEndTime = bucketEndTime;
            this.bucketId = new SampleBucketId(channelDataId, decimationLevel,
                    bucketStartTime);
            this.channelName = channelName;
        }

        /**
         * Returns the end time of this sample bucket as the number of
         * nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC). A bucket
         * only stores sample with time stamps equal to or less than its end
         * time. If there is a following bucket for the same channel and
         * decimation level (a bucket with a greater start time), this bucket's
         * end time must be strictly less than the following bucket's start
         * time.
         * 
         * @return end time of this bucket as the number of nanoseconds since
         *         epoch.
         */
        public long getBucketEndTime() {
            return bucketEndTime;
        }

        /**
         * Returns the unique ID identifiying this sample bucket.
         * 
         * @return unique bucket ID.
         */
        public SampleBucketId getBucketId() {
            return bucketId;
        }

        /**
         * <p>
         * Returns the start time of this sample bucket as the number of
         * nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC). A bucket
         * only stores sample with time stamps equal to or greater than its
         * start time. If there is a preceding bucket for the same channel and
         * decimation level (a bucket with a lesser start time), this bucket's
         * start time must be strictly greater than the preceding bucket's end
         * time.
         * </p>
         * 
         * <p>
         * This number is the same one as the one that is provided as part of
         * the bucket ID returned by {@link #getBucketId()}.
         * </p>
         * 
         * @return start time of this bucket as the number of nanoseconds since
         *         epoch.
         */
        public long getBucketStartTime() {
            return bucketId.getBucketStartTime();
        }

        /**
         * <p>
         * Returns the unique identifier that is associated with the data
         * (samples) for the channel. While a channel's name might change due to
         * renaming, the data ID will be permanent (until the channel is
         * deleted), so even after renaming a channel, its associated data can
         * still be found.
         * </p>
         * 
         * <p>
         * This identifier is the same one as the one that is provided as part
         * of the bucket ID returned by {@link #getBucketId()}.
         * </p>
         * 
         * @return unique identifier associated with the channel's data.
         */
        public UUID getChannelDataId() {
            return bucketId.getChannelDataId();
        }

        /**
         * Returns the name of the channel for which this sample bucket stores
         * data.
         * 
         * @return name identifying the channel to which this bucket belongs.
         */
        public String getChannelName() {
            return channelName;
        }

        /**
         * <p>
         * Returns the decimation level for which this sample bucket stores
         * data. The decimation level is identified by the number of seconds
         * between two samples. A decimation level of zero specifies that this
         * decimation level stores (undecimated) raw samples, which typically do
         * not have a fixed period.
         * </p>
         * 
         * <p>
         * This number is the same one as the one that is provided as part of
         * the bucket ID returned by {@link #getBucketId()}.
         * </p>
         * 
         * @return decimation level of this bucket.
         */
        public int getDecimationLevel() {
            return bucketId.getDecimationLevel();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(bucketEndTime).append(bucketId)
                    .append(channelName).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SampleBucketInformation)) {
                return false;
            }
            SampleBucketInformation other = (SampleBucketInformation) obj;
            return new EqualsBuilder()
                    .append(this.bucketEndTime, other.bucketEndTime)
                    .append(this.bucketId, other.bucketId)
                    .append(this.channelName, other.channelName).isEquals();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("bucketEndTime", bucketEndTime)
                    .append("bucketStartTime", bucketId.getBucketStartTime())
                    .append("channelDataId", bucketId.getChannelDataId())
                    .append("channelName", channelName)
                    .append("decimationLevel", bucketId.getDecimationLevel())
                    .toString();
        }

    }

    /**
     * Creates a channel. The channel must not exist before calling this method.
     * Calling this method for a channel that already exists results in
     * undefined behavior. The calling code has to ensure that the channel does
     * not exist and no other attempt to create it is made concurrently. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @param channelDataId
     *            unique identifier associated with the data (samples) for each
     *            channel. This identifier must be different for each channel.
     *            It is used instead of the channel name when storing data, so
     *            that the channel can be renamed later without losing data.
     * @param controlSystemType
     *            type of the control-system that provides the channel. This
     *            information is used to identify the control-system specific
     *            adapter that is used for the channel.
     * @param decimationLevels
     *            set containing all decimation levels that exist for the
     *            channel.
     * @param decimationLevelToRetentionPeriod
     *            map that contains the retention periods of the decimation
     *            levels to be created. The decimation levels are used as keys
     *            and the corresponding retention periods (in seconds) are
     *            stored as values. A retention period that is zero or negative
     *            means that samples for the corresponding decimation level are
     *            supposed to be retained forever.
     * @param enabled
     *            <code>true</code> if the channel shall be enabled (archived),
     *            <code>false</code> if it should remain disabled. A disabled
     *            channel can be queried for data, but it is not archived.
     * @param options
     *            map containing the options associated with the channel. The
     *            options are forwarded to the control-system specific adapter
     *            when registering the channel with it. The map may not contain
     *            <code>null</code> keys or values.
     * @param serverId
     *            ID of the server that is responsible for the channel.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> createChannel(String channelName,
            UUID channelDataId, String controlSystemType,
            Set<Integer> decimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            boolean enabled, Map<String, String> options, UUID serverId);

    /**
     * Creates decimation levels for the specified channel. The specified
     * channel must already exist and the specified decimation levels must not
     * exist before calling this method. The calling code has to ensure that
     * this preconditions are met and that no other modifications of the channel
     * that might interfere with this change are performed concurrently. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @param serverId
     *            ID of the server that is responsible for the channel.
     * @param decimationLevels
     *            set containing the decimation levels which should be created
     *            for the specified channel. This set must not contain
     *            decimation levels that already exist for the channel.
     * @param decimationLevelToRetentionPeriod
     *            map that contains the retention periods of the decimation
     *            levels to be created. The decimation levels are used as keys
     *            and the corresponding retention periods (in seconds) are
     *            stored as values. A retention period that is zero or negative
     *            means that samples for the corresponding decimation level are
     *            supposed to be retained forever.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> createChannelDecimationLevels(String channelName,
            UUID serverId, Set<Integer> decimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod);

    /**
     * Creates a pending channel operation. The information is stored for the
     * specified server and channel and is automatically deleted after the
     * <code>ttl</code> has passed. If another pending operation for the same
     * server and channel already exists, the existing data is not touched.
     * Instead, this method returns the operation ID of the existing record.
     * This check is guaranteed to be implemented in an atomic way, even across
     * servers. The operation is performed in an asynchronous way so that it
     * will not block for network communication. The result of the operation can
     * be checked through the returned future.
     * 
     * @param serverId
     *            ID of the server that is affected by the operation. This most
     *            often is (but does not have to be) the server responsible for
     *            the affected channel.
     * @param channelName
     *            name of the channel affected by the operation.
     * @param operationId
     *            unique ID identifying the operation. This ID is used when
     *            deleting the operation using the
     *            {@link #deletePendingChannelOperation(UUID, String, UUID)} or
     *            updating it using the
     *            {@link #updatePendingChannelOperation(UUID, String, UUID, UUID, String, String, int)}
     *            .
     * @param operationType
     *            type of the operation. The type also defines the expected
     *            format of the data passed as <code>operationData</code> (if
     *            any). However, the interpretation of both the type and the
     *            data is left to the code using this DAO.
     * @param operationData
     *            data associated with the operation. The format of the data
     *            typically depends on the <code>operationType</code>. May be
     *            <code>null</code>.
     * @param ttl
     *            time-to-live of the record (in seconds). The record is
     *            automatically deleted after the specified amount of seconds.
     *            This means that after this period of time, calls to
     *            {@link #getPendingChannelOperation(UUID, String)} and
     *            {@link #getPendingChannelOperations(UUID)} will not return the
     *            channel operation any longer.
     * @return pair of a boolean and a channel operation ID exposed through a
     *         future. The boolean is <code>true</code> if and only if the
     *         channel operation was successfully stored for the specified
     *         server and channel. If another operation was already stored and
     *         thus the specified operation was not stored, the boolean is
     *         <code>false</code> and the ID of the already stored channel
     *         operation is returned as part of the pair. In case of failure,
     *         the future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Pair<Boolean, UUID>> createPendingChannelOperation(
            UUID serverId, String channelName, UUID operationId,
            String operationType, String operationData, int ttl);

    /**
     * Creates a sample bucket for a channel. This only registers the meta-data
     * and does not store any actual samples. If <code>isNewCurrentBucket</code>
     * is <code>true</code>, the newly created bucket is considered the current
     * bucket to which new samples for the specified decimation level should be
     * added. This means that the stored meta-data is updated so that future
     * queries returning a {@link ChannelConfiguration} object for the channel
     * will contain a reference to this bucket. If
     * <code>precedingBucketStartTime</code> is not <code>null</code>, the end
     * time of the specified preceding bucket is updated with the start time of
     * the new bucket minus one. This is useful when a bucket is appended
     * because the existing, preceding bucket will naturally end just before the
     * start of the new bucket. The operation is performed in an asynchronous
     * way so that it will not block for network communication. The result of
     * the operation can be checked through the returned future.
     * 
     * @param channelName
     *            name identifying the channel for which a sample bucket shall
     *            be added.
     * @param decimationLevel
     *            decimation level for which a sample bucket shall be added.
     * @param bucketStartTime
     *            start time of the new bucket. The start time must be less than
     *            or equal to the time of the first sample supposed to be stored
     *            in the bucket. It must be strictly greater than the time of
     *            the most recent sample stored in the preceding bucket.
     * @param bucketEndTime
     *            end time of the new bucket. The end time must be greater than
     *            or equal to the time of the last sample stored in the bucket.
     *            It must be strictly less than the the start time of any
     *            following bucket (if there is one).
     * @param precedingBucketStartTime
     *            start time of the (existing) preceding sample bucket. If not
     *            <code>null</code>, the end time of that bucket is updated with
     *            the specified <code>bucketStartTime</code> minus one.
     * @param isNewCurrentBucket
     *            <code>true</code> if the bucket shall be used for samples
     *            added to the specified decimation level. This implies that
     *            this is the most recent bucket and there are no following
     *            buckets at the time being. <code>false</code> if this bucket
     *            shall not be used for new samples. This is only useful if a
     *            bucket preceding the already existing buckets is added and
     *            will contain samples that are older than the already existing
     *            samples.
     * @param serverId
     *            ID of the server which is responsible for the channel. This
     *            may be <code>null</code> if <code>isNewCurrentBucket</code> is
     *            <code>false</code>.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> createSampleBucket(String channelName,
            int decimationLevel, long bucketStartTime, long bucketEndTime,
            Long precedingBucketStartTime, boolean isNewCurrentBucket,
            UUID serverId);

    /**
     * Deletes a channel. The channel must exist and be associated with the
     * specified server before calling this method. Calling this method for a
     * channel that does not exist or that is associated with a different server
     * results in undefined behavior. the calling code has to ensure that these
     * preconditions are met and no other attempt to modify the channel are made
     * concurrently. This method only deletes the meta-data for this channel.
     * The calling code should delete all samples for the channel before calling
     * this method because these samples will not be accessible any longer after
     * deleting the meta-data. Any pending channel operation stored for the
     * channel is not affected by this method. The operation is performed in an
     * asynchronous way so that it will not block for network communication. The
     * result of the operation can be checked through the returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @param serverId
     *            ID of the server which is currently responsible for the
     *            channel.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> deleteChannel(String channelName, UUID serverId);

    /**
     * Removes decimation levels from the specified channel. The specified
     * channel must exist and belong to the specified server before calling this
     * method. The calling code has to ensure that this preconditions are met
     * and that no other modifications of the channel that might interfere with
     * this change are performed concurrently. This method also deletes all
     * meta-data for the sample buckets of the specified decimation levels, but
     * it does not delete the actual samples. The calling code should delete
     * those samples before calling this method because they will not be
     * accessible any longer after the meta-data is deleted. The operation is
     * performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @param serverId
     *            ID of the server that is responsible for the channel.
     * @param decimationLevels
     *            set containing the decimation levels which should be removed
     *            from the specified channel.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> deleteChannelDecimationLevels(String channelName,
            UUID serverId, Set<Integer> decimationLevels);

    /**
     * Deletes a pending channel operation associated with the specified server
     * and channel. If the pending operation stored for the specified server and
     * channel has a different ID, it is not deleted. Instead, this method
     * returns the ID of the stored record. This check is guaranteed to be
     * implemented in an atomic way, even across servers. The operation is
     * performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * 
     * @param serverId
     *            ID of the server that is affected by the operation. This most
     *            often is (but does not have to be) the server responsible for
     *            the affected channel.
     * @param channelName
     *            name of the channel affected by the operation.
     * @param operationId
     *            unique ID identifying the operation. This is the ID that was
     *            specified when creating or updating the channel operation.
     * @return pair of a boolean and a channel operation ID exposed through a
     *         future. The boolean is <code>true</code> if and only if the
     *         specified channel operation was deleted or there was no channel
     *         operation stored for the specified server and channel. If a
     *         different channel operation was stored for the specified server
     *         and channel, the boolean is <code>false</code> and the ID of the
     *         stored channel operation is returned as part of the pair. In case
     *         of failure, the future's <code>get()</code> method will throw an
     *         exception.
     */
    ListenableFuture<Pair<Boolean, UUID>> deletePendingChannelOperation(
            UUID serverId, String channelName, UUID operationId);

    /**
     * Deletes the meta-data for a sample bucket. The bucket is identified by
     * its start time. If the bucket is the current bucket for the specified
     * decimation level, the corresponding flag must be set. This ensures that
     * the respective current bucket ID in the {@link ChannelConfiguration} is
     * reset to <code>null</code>. When modifying the current bucket ID, the
     * calling code has to ensure that no other concurrent changes are applied
     * to the current bucket ID for the specified channel and decimation level.
     * The operation is performed in an asynchronous way so that it will not
     * block for network communication. The result of the operation can be
     * checked through the returned future.
     * 
     * @param channelName
     *            name identifying the channel to which the sample bucket
     *            belongs.
     * @param decimationLevel
     *            decimation level containing the sample bucket. A decimation
     *            level is identified by the number of seconds between two
     *            samples. All decimation levels except the zero decimation
     *            level, which stores raw data, have fixed periods between
     *            samples.
     * @param bucketStartTime
     *            start time of the sample bucket.
     * @param isCurrentBucket
     *            <code>true</code> if the bucket is the current bucket for the
     *            specified decimation level. This has the consequence that the
     *            decimation level's current bucket ID is reset to
     *            <code>null</code>. If <code>false</code>, the current bucket
     *            ID is not touched.
     * @param serverId
     *            ID of the server which is currently responsible for the
     *            channel. If <code>isCurrentBucket</code> is <code>false</code>
     *            , this parameter is not used and may be <code>null</code>.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> deleteSampleBucket(String channelName,
            int decimationLevel, long bucketStartTime, boolean isCurrentBucket,
            UUID serverId);

    /**
     * Returns the meta-data for the specified channel. The operation is
     * performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @return general meta-data for the specified channel exposed through a
     *         future. If no such channel exists, the future's <code>get</code>
     *         method returns <code>null</code>. In case of failure, the
     *         future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<ChannelInformation> getChannel(String channelName);

    /**
     * Returns the channel configuration for the specified channel belonging to
     * the specified server. The configuration is the meta-data associated with
     * the channel that is needed by a server when initializing the channel. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * 
     * @param serverId
     *            ID of the server responsible for the channel.
     * @param channelName
     *            name identifying the requested channel.
     * @return configuration for the specified channel exposed through a future.
     *         If no such channel exists, the future's <code>get</code> method
     *         returns <code>null</code>. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<ChannelConfiguration> getChannelByServer(UUID serverId,
            String channelName);

    /**
     * <p>
     * Returns the meta-data for all channels. The information is returned in no
     * particular order. The operation is performed in an asynchronous way so
     * that it will not block for network communication. The result of the
     * operation can be checked through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @return general meta-data for all channels exposed through a future. In
     *         case of failure, the future's <code>get()</code> method will
     *         throw an exception.
     */
    ListenableFuture<? extends Iterable<ChannelInformation>> getChannels();

    /**
     * <p>
     * Returns the configuration for all channels belonging to the specified
     * server. The configuration is the meta-data associated with the channel
     * that is needed by a server when initializing the channel. The operation
     * is performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param serverId
     *            ID of the server for which the channels shall be retrieved.
     * @return configuration for all channels belonging to the specified server,
     *         exposed through a future. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<? extends Iterable<ChannelConfiguration>> getChannelsByServer(
            UUID serverId);

    /**
     * Returns the pending channel operation for the specified server and
     * channel. The operation is performed in an asynchronous way so that it
     * will not block for network communication. The result of the operation can
     * be checked through the returned future.
     * 
     * @param serverId
     *            ID of the server that is affected by the operation. This most
     *            often is (but does not have to be) the server responsible for
     *            the affected channel.
     * @param channelName
     *            name of the channel affected by the operation.
     * @return pending channel operation for the specified server and channel
     *         exposed through a future. If no such operation has been stored
     *         (or it has already been deleted), the future's <code>get</code>
     *         method returns <code>null</code>. In case of failure, the
     *         future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<ChannelOperation> getPendingChannelOperation(
            UUID serverId, String channelName);

    /**
     * <p>
     * Returns all pending channel operations for the specified server. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param serverId
     *            ID of the server that is affected by the operation. This most
     *            often is (but does not have to be) the server responsible for
     *            the affected channel.
     * @return all pending channel operations for the specified server exposed
     *         through a future. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<? extends Iterable<ChannelOperation>> getPendingChannelOperations(
            UUID serverId);

    /**
     * <p>
     * Returns the meta data for all sample buckets for the specified channel
     * and all decimation levels. No particular order is guaranteed. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which all sample buckets
     *            shall be returned.
     * @return meta-data for all sample buckets for the specified channel and
     *         all decimation levels exposed through a future. In case of
     *         failure, the future's <code>get()</code> method will throw an
     *         exception.
     */
    ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBuckets(
            String channelName);

    /**
     * <p>
     * Returns the meta data for all sample buckets for the specified channel
     * and decimation level. The sample buckets are sorted by their start time
     * in ascending order. The operation is performed in an asynchronous way so
     * that it will not block for network communication. The result of the
     * operation can be checked through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which the sample buckets
     *            shall be returned.
     * @param decimationLevel
     *            decimation level for which the sample buckets shall be
     *            returned. A decimation level is identified by the number of
     *            seconds between two samples. All decimation levels except the
     *            zero decimation level, which stores raw data, have fixed
     *            periods between samples.
     * @return meta-data for all sample buckets for the specified channel and
     *         decimation level exposed through a future. In case of failure,
     *         the future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBuckets(
            String channelName, int decimationLevel);

    /**
     * <p>
     * Returns the meta data for the sample buckets for the specified channel
     * and decimation level which are in the specified interval. Only those
     * sample buckets that have a start time that is in the specified interval
     * are returned. The sample buckets are sorted by their start time in
     * ascending order. The operation is performed in an asynchronous way so
     * that it will not block for network communication. The result of the
     * operation can be checked through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which the sample buckets
     *            shall be returned.
     * @param decimationLevel
     *            decimation level for which the sample buckets shall be
     *            returned. A decimation level is identified by the number of
     *            seconds between two samples. All decimation levels except the
     *            zero decimation level, which stores raw data, have fixed
     *            periods between samples.
     * @param startTimeGreaterThanOrEqualTo
     *            lower limit for the start time (inclusive).
     * @param startTimeLessThanOrEqualTo
     *            upper limit for the start time (inclusive).
     * @return meta-data for the sample buckets for the specified channel,
     *         decimation level, and interval exposed through a future. In case
     *         of failure, the future's <code>get()</code> method will throw an
     *         exception.
     */
    ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsInInterval(
            String channelName, int decimationLevel,
            long startTimeGreaterThanOrEqualTo, long startTimeLessThanOrEqualTo);

    /**
     * <p>
     * Returns the meta data for all sample buckets for the specified channel
     * and decimation level up to the specified limit. The sample buckets are
     * sorted by their start time in descending order. At maximum, the specified
     * number of sample buckets is returned. The operation is performed in an
     * asynchronous way so that it will not block for network communication. The
     * result of the operation can be checked through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which the sample buckets
     *            shall be returned.
     * @param decimationLevel
     *            decimation level for which the sample buckets shall be
     *            returned. A decimation level is identified by the number of
     *            seconds between two samples. All decimation levels except the
     *            zero decimation level, which stores raw data, have fixed
     *            periods between samples.
     * @param limit
     *            maximum number of sample buckets which should be included in
     *            the result. If there are more buckets, only the first
     *            <code>limit</code> buckets are returned.
     * @return meta-data for all sample buckets for the specified channel and
     *         decimation level exposed through a future. In case of failure,
     *         the future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsInReverseOrder(
            String channelName, int decimationLevel, int limit);

    /**
     * <p>
     * Returns the meta data for the sample buckets for the specified channel
     * and decimation level which have a start time later than the specified
     * time. At maximum, the specified number of sample buckets is returned. The
     * sample buckets are sorted by their start time in ascending order. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which the sample buckets
     *            shall be returned.
     * @param decimationLevel
     *            decimation level for which the sample buckets shall be
     *            returned. A decimation level is identified by the number of
     *            seconds between two samples. All decimation levels except the
     *            zero decimation level, which stores raw data, have fixed
     *            periods between samples.
     * @param startTimeGreaterThanOrEqualTo
     *            lower limit for the start time (inclusive).
     * @param limit
     *            maximum number of sample buckets which should be included in
     *            the result. If there are more buckets, only the first
     *            <code>limit</code> buckets are returned.
     * @return meta-data for the sample buckets for the specified channel,
     *         decimation level, and interval exposed through a future. In case
     *         of failure, the future's <code>get()</code> method will throw an
     *         exception.
     */
    ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsNewerThan(
            String channelName, int decimationLevel,
            long startTimeGreaterThanOrEqualTo, int limit);

    /**
     * <p>
     * Returns the meta data for the sample buckets for the specified channel
     * and decimation level which have a start time before the specified time.
     * At maximum, the specified number of sample buckets is returned. The
     * sample buckets are sorted by their start time in descending order. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @param channelName
     *            name identifying the channel for which the sample buckets
     *            shall be returned.
     * @param decimationLevel
     *            decimation level for which the sample buckets shall be
     *            returned. A decimation level is identified by the number of
     *            seconds between two samples. All decimation levels except the
     *            zero decimation level, which stores raw data, have fixed
     *            periods between samples.
     * @param startTimeLessThanOrEqualTo
     *            upper limit for the start time.
     * @param limit
     *            maximum number of sample buckets which should be included in
     *            the result. If there are more buckets, only the first
     *            <code>limit</code> buckets are returned.
     * @return meta-data for the sample buckets for the specified channel,
     *         decimation level, and interval exposed through a future. In case
     *         of failure, the future's <code>get()</code> method will throw an
     *         exception.
     */
    ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsOlderThanInReverseOrder(
            String channelName, int decimationLevel,
            long startTimeLessThanOrEqualTo, int limit);

    /**
     * Moves a channel from one server to another. The channel must exist and be
     * associated with the specified server before calling this method. Calling
     * this method for a channel that does not exists yet or is associated with
     * a different server results in undefined behavior. The calling code has to
     * ensure that these preconditions are met and no other attempts to modify
     * the channel are made concurrently. This operation does not affect any
     * pending channel operations stored for the specified channel. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @param oldServerId
     *            ID of the server which is currently responsible for the
     *            channel.
     * @param newServerId
     *            ID of the server which is supposed to be responsible for the
     *            channel after the move operation finishes.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> moveChannel(String channelName, UUID oldServerId,
            UUID newServerId);

    /**
     * Changes the name of a channel. This effectively copies all meta-data for
     * the channel (and its sample buckets) and removes the data for the old
     * name. This operation is not atomic. This means that the calling code has
     * to ensure that no concurrent modifications are made to the channel
     * (including the creation or deletion of sample buckets) under both its old
     * and new name. If no channel with the specified old name exists or if a
     * channel with the specified new name already exists, the behavior is
     * undefined. This operation does not affect any pending channel operations
     * stored for the specified channel. The operation is performed in an
     * asynchronous way so that it will not block for network communication. The
     * result of the operation can be checked through the returned future.
     * 
     * @param oldChannelName
     *            old name identifying the existing channel.
     * @param newChannelName
     *            new name that identifies the channel after the rename
     *            operation finishes.
     * @param serverId
     *            ID of the server responsible for the channel.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> renameChannel(String oldChannelName,
            String newChannelName, UUID serverId);

    /**
     * Updates the configuration for a channel. The calling code has to ensure
     * that the specified channel exists and is associated with the specified
     * server. Otherwise, the behavior of this method is undefined. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * 
     * @param channelName
     *            name identifying the channel.
     * @param serverId
     *            ID of the server responsible for the channel.
     * @param decimationLevelToRetentionPeriod
     *            map that contains the retention periods of the decimation
     *            levels that shall be updated. The decimation levels are used
     *            as keys and the corresponding retention periods (in seconds)
     *            are stored as values. A retention period that is zero or
     *            negative means that samples for the corresponding decimation
     *            level are supposed to be retained forever. This map may only
     *            contain the retention level for decimation periods that have
     *            been created previously. If it contains information for a
     *            decimation level that does not exist, the behavior is
     *            undefined. However, if this map does not contain information
     *            for an existing decimation level, the retention period of this
     *            decimation level is simply not changed.
     * @param enabled
     *            <code>true</code> if archiving shall be enabled for the
     *            channel, <code>false</code> if it shall be disabled.
     * @param options
     *            control-system specific configuration options for the channel.
     *            The map may not contain <code>null</code> keys or values.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> updateChannelConfiguration(String channelName,
            UUID serverId,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            boolean enabled, Map<String, String> options);

    /**
     * Updates an existing pending channel operation, replacing it with the
     * specified information. If there is no pending channel operation matching
     * the specified server, channel, and operation ID, no data is modified.
     * Instead, this method returns the ID of the channel operation stored for
     * the specified server and channel (if any). This check is guaranteed to be
     * implemented in an atomic way, even across servers. The operation is
     * performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * 
     * @param serverId
     *            ID of the server that is affected by the operation. This most
     *            often is (but does not have to be) the server responsible for
     *            the affected channel.
     * @param channelName
     *            name of the channel affected by the operation.
     * @param oldOperationId
     *            ID of the operation that shall be replaced with the specified
     *            new information. The replacement is only made if the ID of the
     *            stored operation matched the specified ID.
     * @param newOperationId
     *            ID of the operation that shall replace the stored operation.
     *            This may (but does not have to) be the same ID as the one of
     *            the old (previously stored) operation.
     * @param newOperationType
     *            type of the new operation. The type also defines the expected
     *            format of the data passed as <code>newOperationData</code> (if
     *            any). However, the interpretation of both the type and the
     *            data is left to the code using this DAO.
     * @param newOperationData
     *            data associated with the new operation. The format of the data
     *            typically depends on the <code>newOperationType</code>. May be
     *            <code>null</code>.
     * @param ttl
     *            time-to-live of the record (in seconds). The record is
     *            automatically deleted after the specified amount of seconds.
     *            This means that after this period of time, calls to
     *            {@link #getPendingChannelOperation(UUID, String)} and
     *            {@link #getPendingChannelOperations(UUID)} will not return the
     *            channel operation any longer.
     * @return pair of a boolean and a channel operation ID exposed through a
     *         future. The boolean is <code>true</code> if and only if the
     *         channel operation was successfully updated with the specified
     *         information. If the ID of the operation stored for the server and
     *         channel was different from the specified ID, the boolean is
     *         <code>false</code> and the ID of the already stored operation is
     *         returned as part of the pair. If there was no operation stored
     *         for the specified server and channel, the boolean is
     *         <code>false</code>, but there is no channel operation ID in the
     *         pair (the corresponding field is <code>null</code>).
     */
    ListenableFuture<Pair<Boolean, UUID>> updatePendingChannelOperation(
            UUID serverId, String channelName, UUID oldOperationId,
            UUID newOperationId, String newOperationType,
            String newOperationData, int ttl);

}
