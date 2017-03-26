/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.compose;
import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.ifThen;
import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.ifThenElse;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.NoSuchChannelException;
import com.aquenos.cassandra.pvarchiver.server.archiving.PendingChannelOperationException;
import com.aquenos.cassandra.pvarchiver.server.archiving.RefreshChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.UpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.SampleBucketInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Asynchronous operation for updating a channel. The operation is encapsulated
 * in a class because it consists of many steps that are chained in an
 * asynchronous fashion in order to avoid blocking a thread for an extended
 * amount of time. This class is only intended for use by the
 * {@link ArchiveConfigurationService}.
 * 
 * @author Sebastian Marsching
 */
public final class UpdateChannelOperation {

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> CREATE_DECIMATION_LEVELS = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            return Futures.transform(
                    operation.channelMetaDataDAO.createChannelDecimationLevels(
                            operation.channelName, operation.serverId,
                            operation.addDecimationLevels,
                            operation.decimationLevelToRetentionPeriod != null
                                    ? operation.decimationLevelToRetentionPeriod
                                    : Collections
                                            .<Integer, Integer> emptyMap()),
                    operation.returnThis);
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> CREATE_PENDING_OPERATION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            operation.operationId = UUID.randomUUID();
            operation.pendingOperationCreatedTime = System.currentTimeMillis();
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(operation.serverId,
                            operation.channelName, operation.operationId,
                            PendingChannelOperationConstants.OPERATION_UPDATE,
                            "",
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures.transform(future,
                    new AsyncFunction<Pair<Boolean, UUID>, UpdateChannelOperation>() {
                        @Override
                        public ListenableFuture<UpdateChannelOperation> apply(
                                Pair<Boolean, UUID> input) throws Exception {
                            operation.createdPendingOperation = input.getLeft();
                            return operation.immediateFutureForThis;
                        }
                    });
        }
    };

    private static final Predicate<UpdateChannelOperation> CREATED_PENDING_OPERATION = new Predicate<UpdateChannelOperation>() {
        @Override
        public boolean apply(UpdateChannelOperation operation) {
            return operation.createdPendingOperation;
        }
    };

    private static final Predicate<UpdateChannelOperation> CONTROL_SYSTEM_SUPPORT_MISSING = new Predicate<UpdateChannelOperation>() {
        @Override
        public boolean apply(UpdateChannelOperation operation) {
            return operation.controlSystemSupport == null;
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> DELETE_DECIMATION_LEVELS = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            return Futures.transform(
                    operation.channelMetaDataDAO.deleteChannelDecimationLevels(
                            operation.channelName, operation.serverId,
                            operation.removeDecimationLevels),
                    operation.returnThis);
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> DELETE_PENDING_OPERATION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // If this operation did not run on the server that owns the
            // channel, we do not simply delete the pending operation but
            // replace it with a short-lived pending operation. This ensures
            // that the channel is not modified by a different server shortly
            // after modifying it. Such a modification could be critical if
            // there is a clock skew (the modification might have a smaller time
            // stamp than the time stamp associated with our modifications and
            // thus the later modifications might get discarded. If the channel
            // was not modified we obviously can skip this. We also can skip it
            // if the operation ran on the correct server and the server is
            // online because in this case future operations will either run on
            // the same server (thus using the same clock) or will be delayed
            // until the server is considered "offline".
            ListenableFuture<Pair<Boolean, UUID>> future;
            if (operation.modified && (!operation.thisServerId
                    .equals(operation.serverId)
                    || !operation.clusterManagementService.isOnline())) {
                future = operation.channelMetaDataDAO
                        .updatePendingChannelOperation(operation.serverId,
                                operation.channelName, operation.operationId,
                                UUID.randomUUID(),
                                PendingChannelOperationConstants.OPERATION_PROTECTIVE,
                                "{ serverId: \"" + operation.thisServerId
                                        + "\", currentSystemTime: \""
                                        + System.currentTimeMillis() + "\" }",
                                PendingChannelOperationConstants.PROTECTIVE_PENDING_OPERATION_TTL_SECONDS);
            } else {
                future = operation.channelMetaDataDAO
                        .deletePendingChannelOperation(operation.serverId,
                                operation.channelName, operation.operationId);
            }
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> DELETE_SAMPLE_BUCKETS = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // We have to delete the actual sample buckets before deleting the
            // meta-data. Otherwise, there would be no way to delete them later
            // because we would not be able to identify them.
            // After receiving the list of buckets, we run the function using
            // the passed executor. If a channel has a lot of samples, the list
            // of sample buckets might be long and we might spend a considerable
            // amount of time starting the delete operations.
            operation.modified = true;
            AsyncFunction<Iterable<? extends SampleBucketInformation>, List<Void>> deleteSampleBuckets = new AsyncFunction<Iterable<? extends SampleBucketInformation>, List<Void>>() {
                @Override
                public ListenableFuture<List<Void>> apply(
                        Iterable<? extends SampleBucketInformation> input)
                        throws Exception {
                    LinkedList<ListenableFuture<Void>> deleteSampleBucketFutures = new LinkedList<ListenableFuture<Void>>();
                    for (SampleBucketInformation sampleBucketInformation : input) {
                        // If deleteSamples throws an exception (it should not),
                        // this method throws an exception and the whole
                        // operation will fail. This is okay because such a
                        // behavior violates the contract of the
                        // ControlSystemSupport interface.
                        ListenableFuture<Void> future = operation.controlSystemSupport
                                .deleteSamples(
                                        sampleBucketInformation.getBucketId());
                        // deleteSamples should not return null either.
                        if (future == null) {
                            throw new NullPointerException(
                                    "The control-system support's deleteSamples methods returned null.");
                        }
                        deleteSampleBucketFutures.add(future);
                    }
                    return Futures.allAsList(deleteSampleBucketFutures);
                }
            };
            LinkedList<ListenableFuture<List<Void>>> deleteSampleBucketsFutures = new LinkedList<ListenableFuture<List<Void>>>();
            for (int decimationLevel : operation.removeDecimationLevels) {
                deleteSampleBucketsFutures.add(Futures.transform(
                        operation.channelMetaDataDAO.getSampleBuckets(
                                operation.channelName, decimationLevel),
                        deleteSampleBuckets, operation.executor));
            }
            return Futures.transform(
                    Futures.allAsList(deleteSampleBucketsFutures),
                    operation.returnThis);
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> FORWARD_TO_SERVER = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            return Futures.transform(
                    operation.interNodeCommunicationService
                            .runArchiveConfigurationCommands(
                                    operation.serverBaseUrl,
                                    Collections
                                            .singletonList(operation.command)),
                    new Function<List<ArchiveConfigurationCommandResult>, UpdateChannelOperation>() {
                        @Override
                        public UpdateChannelOperation apply(
                                List<ArchiveConfigurationCommandResult> input) {
                            assert (input.size() == 1);
                            ArchiveConfigurationCommandResult result = input
                                    .get(0);
                            if (result.isSuccess()) {
                                return operation;
                            } else {
                                throw new RuntimeException("Server "
                                        + operation.serverId
                                        + " reported an error when trying to update channel \""
                                        + StringEscapeUtils.escapeJava(
                                                operation.channelName)
                                        + "\""
                                        + (result.getErrorMessage() != null
                                                ? ": " + result
                                                        .getErrorMessage()
                                                : "."));
                            }
                        }
                    });
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> GET_CHANNEL_CONFIGURATION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // If we did not get the channel information, we cannot get the
            // channel configuration because we need the server ID. However,
            // this is okay because the following code will throw an exception
            // if the channel information is null before using the channel
            // configuration.
            if (operation.channelInformation == null) {
                return operation.immediateFutureForThis;
            }
            return Futures.transform(
                    operation.channelMetaDataDAO.getChannelByServer(
                            operation.channelInformation.getServerId(),
                            operation.channelName),
                    new Function<ChannelConfiguration, UpdateChannelOperation>() {
                        @Override
                        public UpdateChannelOperation apply(
                                ChannelConfiguration input) {
                            operation.channelConfiguration = input;
                            // The channel configuration may be null if the
                            // channel has been deleted or moved in the
                            // meantime. This will be handled correctly in
                            // UPDATE_CHANNEL_STEP1 and UPDATE_CHANNEL_STEP3,
                            // so we simply skip the rest of the logic here
                            // because we cannot compare the existing with the
                            // target configuration if there is no existing
                            // configuration.
                            if (operation.channelConfiguration == null) {
                                return operation;
                            }
                            operation.controlSystemSupport = operation.controlSystemSupportRegistry
                                    .getControlSystemSupport(
                                            operation.channelConfiguration
                                                    .getControlSystemType());
                            Set<Integer> existingDecimationLevels = operation.channelConfiguration
                                    .getDecimationLevelToCurrentBucketStartTime()
                                    .keySet();
                            boolean decimationLevelsModified = ((operation.decimationLevels != null
                                    && !operation.decimationLevels
                                            .equals(existingDecimationLevels))
                                    || Sets.intersection(
                                            existingDecimationLevels,
                                            operation.addDecimationLevels)
                                            .size() != operation.addDecimationLevels
                                                    .size()
                                    || !Sets.intersection(
                                            existingDecimationLevels,
                                            operation.removeDecimationLevels)
                                            .isEmpty());
                            Map<Integer, Integer> existingDecimationLevelToRetentionPeriod = operation.channelConfiguration
                                    .getDecimationLevelToRetentionPeriod();
                            boolean decimationLevelRetentionPeriodsModified = (operation.decimationLevelToRetentionPeriod != null
                                    && !Maps.difference(
                                            existingDecimationLevelToRetentionPeriod,
                                            operation.decimationLevelToRetentionPeriod)
                                            .entriesDiffering().isEmpty());
                            boolean enabledModified = (operation.enabled != null
                                    && operation.enabled != operation.channelConfiguration
                                            .isEnabled());
                            Map<String, String> existingOptions = operation.channelConfiguration
                                    .getOptions();
                            boolean optionsModified = ((operation.options != null
                                    && !operation.options
                                            .equals(existingOptions))
                                    || !Maps.difference(existingOptions,
                                            operation.addOptions)
                                            .entriesOnlyOnRight()
                                            .isEmpty()
                                    || !Maps.difference(existingOptions,
                                            operation.addOptions)
                                            .entriesDiffering()
                                            .isEmpty()
                                    || !Sets.intersection(
                                            existingOptions.keySet(),
                                            operation.removeOptions).isEmpty());
                            operation.configurationModified = (decimationLevelsModified
                                    || decimationLevelRetentionPeriodsModified
                                    || enabledModified || optionsModified);
                            // We have to verify that after applying the
                            // updates, there still are no decimation levels
                            // that have a retention period less than the one of
                            // the preceding decimation level.
                            // If all decimation levels are going to be
                            // replaced, the retention periods have already been
                            // checked and we can continue without a check.
                            // If existing retention periods might be kept, we
                            // have to check whether the existing retention
                            // periods (after removing the decimation levels
                            // that are going to be removed) are compatible with
                            // the new retention periods. The new retention
                            // periods have already been filtered to not contain
                            // periods for decimation levels that are going to
                            // be removed, so we only have to filter the
                            // existing ones.
                            operation.verifyRetentionPeriodsException = null;
                            if (operation.decimationLevels == null) {
                                TreeMap<Integer, Integer> mergedDecimationLevelToRetentionPeriod = new TreeMap<Integer, Integer>();
                                mergedDecimationLevelToRetentionPeriod
                                        .putAll(Maps.filterKeys(
                                                existingDecimationLevelToRetentionPeriod,
                                                Predicates.not(Predicates.in(
                                                        operation.removeDecimationLevels))));
                                // Updated retention periods will win over
                                // existing ones, so we add them second.
                                mergedDecimationLevelToRetentionPeriod.putAll(
                                        operation.decimationLevelToRetentionPeriod);
                                try {
                                    ArchiveConfigurationUtils
                                            .verifyRetentionPeriodsAscending(
                                                    mergedDecimationLevelToRetentionPeriod);
                                } catch (RuntimeException e) {
                                    // We store the exception so that it can be
                                    // used in one of the next steps. We do not
                                    // throw it directly, because there might be
                                    // a pending operation that first has to be
                                    // cleaned up.
                                    operation.verifyRetentionPeriodsException = e;
                                }
                            }
                            return operation;
                        }
                    });
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> GET_CHANNEL_INFORMATION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            return Futures.transform(
                    operation.channelInformationCache
                            .getChannelAsync(operation.channelName, true),
                    new Function<ChannelInformation, UpdateChannelOperation>() {
                        @Override
                        public UpdateChannelOperation apply(
                                ChannelInformation input) {
                            operation.channelInformation = input;
                            operation.needChannelInformation = false;
                            return operation;
                        }
                    });
        }
    };

    private static final Function<UpdateChannelOperation, Boolean> IS_MODIFIED = new Function<UpdateChannelOperation, Boolean>() {
        @Override
        public Boolean apply(UpdateChannelOperation input) {
            return input.modified;
        }
    };

    private static final Predicate<UpdateChannelOperation> NEED_CHANNEL_INFORMATION = new Predicate<UpdateChannelOperation>() {
        @Override
        public boolean apply(UpdateChannelOperation operation) {
            return operation.needChannelInformation;
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> PREPARE_CREATE_DELETE_DECIMATION_LEVELS = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // If the set of decimation levels has not been specified
            // explicitly, we calculate it based on the set of existing
            // decimation levels and the set of decimation levels to be added
            // and to be removed.
            if (operation.decimationLevels == null) {
                operation.decimationLevels = new TreeSet<Integer>(
                        operation.channelConfiguration
                                .getDecimationLevelToCurrentBucketStartTime()
                                .keySet());
                if (operation.addDecimationLevels != null) {
                    operation.decimationLevels
                            .addAll(operation.addDecimationLevels);
                }
                if (operation.removeDecimationLevels != null) {
                    operation.decimationLevels
                            .removeAll(operation.removeDecimationLevels);
                }
            }
            // We have to calculate which decimation levels shall be added and
            // which ones shall be removed. We do this even if the set of
            // decimation levels has not been specified explicitly because we
            // do not want to add decimation levels that already exist and we do
            // not want to remove ones that do not exist.
            operation.addDecimationLevels = Sets.difference(
                    operation.decimationLevels,
                    operation.channelConfiguration
                            .getDecimationLevelToCurrentBucketStartTime()
                            .keySet());
            operation.removeDecimationLevels = Sets
                    .difference(operation.channelConfiguration
                            .getDecimationLevelToCurrentBucketStartTime()
                            .keySet(), operation.decimationLevels);
            return operation.immediateFutureForThis;
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> REFRESH_CHANNEL = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            if (operation.thisServerId.equals(operation.serverId)) {
                return Futures
                        .transform(
                                operation.archivingService
                                        .refreshChannel(operation.channelName),
                                operation.returnThis);
            } else {
                String targetServerBaseUrl = operation.clusterManagementService
                        .getInterNodeCommunicationUrl(operation.serverId);
                if (targetServerBaseUrl != null) {
                    return Futures.transform(
                            operation.interNodeCommunicationService
                                    .runArchiveConfigurationCommands(
                                            targetServerBaseUrl,
                                            Collections.singletonList(
                                                    new RefreshChannelCommand(
                                                            operation.channelName,
                                                            operation.serverId))),
                            operation.returnThis);
                } else {
                    return operation.immediateFutureForThis;
                }
            }
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> THROW_CONTROL_SYSTEM_SUPPORT_MISSING = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            throw new IllegalStateException("The control-sytem support \""
                    + StringEscapeUtils.escapeJava(
                            operation.channelInformation.getControlSystemType())
                    + "\" is not available.");
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> THROW_IF_TIMEOUT = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // If more than two minutes have passed since creating the pending
            // operation, something is going wrong. If we continued, we would
            // risk making changes after the pending operation has expired, so
            // we rather throw an exception.
            if (System
                    .currentTimeMillis() > operation.pendingOperationCreatedTime
                            + 120000L) {
                throw new RuntimeException("Channel \""
                        + StringEscapeUtils.escapeJava(operation.channelName)
                        + "\" cannot be updated because the operation took too long to finish.");
            } else {
                return operation.immediateFutureForThis;
            }
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> THROW_NO_SUCH_CHANNEL_EXCEPTION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            throw new NoSuchChannelException("Channel \""
                    + StringEscapeUtils.escapeJava(operation.channelName)
                    + "\" cannot be updated because it does not exist.");
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> THROW_PENDING_OPERATION_EXCEPTION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            throw new PendingChannelOperationException("Channel \""
                    + StringEscapeUtils.escapeJava(operation.channelName)
                    + "\" cannot be updated because another operation for this channel is pending.");
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> THROW_WRONG_CONTROL_SYSTEM_TYPE = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            throw new IllegalArgumentException("The channel \""
                    + StringEscapeUtils.escapeJava(operation.channelName)
                    + "\" has the control-system type \""
                    + StringEscapeUtils.escapeJava(operation.controlSystemType)
                    + "\", but the control-system type \""
                    + StringEscapeUtils
                            .escapeJava(operation.expectedControlSystemType)
                    + "\" was specified.");
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> THROW_VERIFY_RETENTION_PERIODS_EXCEPTION = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            throw operation.verifyRetentionPeriodsException;
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP1 = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // We can only update a channel that already exists, so if we did
            // not get the channel information, we throw an exception.
            if (operation.channelInformation == null) {
                return THROW_NO_SUCH_CHANNEL_EXCEPTION.apply(operation);
            }
            // If the control-system type does not match the expected one, we
            // throw an exception. We cannot change the control-system type, so
            // this is the only reasonable thing to do.
            operation.controlSystemType = operation.channelInformation
                    .getControlSystemType();
            if (operation.expectedControlSystemType != null
                    && !operation.expectedControlSystemType
                            .equals(operation.controlSystemType)) {
                return THROW_WRONG_CONTROL_SYSTEM_TYPE.apply(operation);
            }
            operation.serverId = operation.channelInformation.getServerId();
            if (operation.expectedServerId != null
                    && !operation.expectedServerId.equals(operation.serverId)) {
                throw new IllegalArgumentException("The channel \""
                        + StringEscapeUtils.escapeJava(operation.channelName)
                        + "\" is registered with the server "
                        + operation.serverId + ", but the server "
                        + operation.expectedServerId + " was specified.");
            }
            // If we did not get the channel configuration, we have to get the
            // channel information again: It is possible that the channel has
            // been moved. If, in the second attempt, we cannot get the channel
            // information either, the channel has been deleted and we throw an
            // exception. We have to reset the needChannelInformation flag so
            // that the channel information is requested again.
            if (operation.channelConfiguration == null) {
                operation.needChannelInformation = true;
                return UPDATE_CHANNEL.apply(operation);
            }
            // If we got the channel configuration and there are no
            // modifications, we are done.
            if (!operation.configurationModified) {
                return operation.immediateFutureForThis;
            }
            // If the verification of the retention periods failed, we cannot
            // continue. Instead, we throw the stored exception.
            if (operation.verifyRetentionPeriodsException != null) {
                return THROW_VERIFY_RETENTION_PERIODS_EXCEPTION
                        .apply(operation);
            }
            // If the channel belongs to a different server and that server is
            // online, we have to delegate the operation to that server.
            operation.serverBaseUrl = operation.clusterManagementService
                    .getInterNodeCommunicationUrl(operation.serverId);
            if (!operation.thisServerId.equals(operation.serverId)
                    && operation.serverBaseUrl != null) {
                return FORWARD_TO_SERVER.apply(operation);
            } else {
                return UPDATE_CHANNEL_STEP2.apply(operation);
            }
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP3 = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // We can only update a channel that already exists. If we did not
            // get a channel configuration, the channel has been deleted or
            // moved after we got the initial information. Anyway, we have to
            // delete the pending operation and check the channel information so
            // that we can use the new server ID if the channel has moved. We
            // set the needChannelInformation flag so that the cached channel
            // information is not used.
            if (operation.channelConfiguration == null) {
                operation.needChannelInformation = true;
                return UPDATE_CHANNEL_STEP4B.apply(operation);
            }
            // The channel configuration might have changed since we got it the
            // last time (before creating the pending operation). Therefore, we
            // basically have to run all checks again.
            // If the control-system type does not match the expected one, we
            // throw an exception. We cannot change the control-system type, so
            // this is the only reasonable thing to do. However, before throwing
            // the exception, we delete the pending operation.
            operation.controlSystemType = operation.channelConfiguration
                    .getControlSystemType();
            if (operation.expectedControlSystemType != null
                    && !operation.expectedControlSystemType
                            .equals(operation.controlSystemType)) {
                return UPDATE_CHANNEL_STEP4A.apply(operation);
            }
            // If we got the channel configuration and there are no
            // modifications, we are done. However, we still have to delete the
            // pending operation.
            if (!operation.configurationModified) {
                return DELETE_PENDING_OPERATION.apply(operation);
            }
            // If the verification of the retention periods failed, we cannot
            // continue. Instead, we throw the stored exception. However, before
            // throwing the exception, we delete the pending operation.
            if (operation.verifyRetentionPeriodsException != null) {
                return UPDATE_CHANNEL_STEP4C.apply(operation);
            }
            // If the channel is not registered with this server, we have to
            // make sure that the owning server is still offline.
            if (!operation.thisServerId.equals(operation.serverId)) {
                return UPDATE_CHANNEL_STEP4D.apply(operation);
            } else {
                return UPDATE_CHANNEL_STEP4E.apply(operation);
            }
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP5 = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            // We filter the retention periods based on the decimation levels
            // that actually exist. Updating the retention period for decimation
            // levels that do not exist has undefined behavior and may thus
            // result in undesired results. We can use the decimationLevels
            // before we calculated the correct set of decimation levels in
            // PREPARE_CREATE_DELETE_DECIMATION_LEVELS.
            Map<Integer, Integer> updatedDecimationLevelToRetentionPeriod = ((operation.decimationLevelToRetentionPeriod != null)
                    ? Maps.filterKeys(
                            operation.decimationLevelToRetentionPeriod,
                            Predicates.in(operation.decimationLevels))
                    : Collections.<Integer, Integer> emptyMap());
            boolean updatedEnabled = (operation.enabled != null)
                    ? operation.enabled
                    : operation.channelConfiguration.isEnabled();
            Map<String, String> updatedOptions;
            if (operation.options != null) {
                updatedOptions = operation.options;
            } else {
                updatedOptions = new TreeMap<String, String>(
                        operation.channelConfiguration.getOptions());
                updatedOptions.putAll(operation.addOptions);
                for (String key : operation.removeOptions) {
                    updatedOptions.remove(key);
                }
            }
            return Futures.transform(Futures.transform(
                    operation.channelMetaDataDAO.updateChannelConfiguration(
                            operation.channelName, operation.serverId,
                            updatedDecimationLevelToRetentionPeriod,
                            updatedEnabled, updatedOptions),
                    operation.returnThis), UPDATE_CHANNEL_STEP6);
        }
    };

    private static final Predicate<UpdateChannelOperation> VERIFIED_SERVER_OFFLINE = new Predicate<UpdateChannelOperation>() {
        @Override
        public boolean apply(UpdateChannelOperation operation) {
            return operation.verifiedServerOffline;
        }
    };

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> VERIFY_SERVER_OFFLINE = new AsyncFunction<UpdateChannelOperation, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(
                final UpdateChannelOperation operation) throws Exception {
            return Futures.transform(
                    operation.clusterManagementService
                            .verifyServerOffline(operation.serverId),
                    new AsyncFunction<Boolean, UpdateChannelOperation>() {
                        @Override
                        public ListenableFuture<UpdateChannelOperation> apply(
                                Boolean input) throws Exception {
                            operation.verifiedServerOffline = input;
                            // There is an inherent race condition here, but we
                            // only verify whether the server is offline if we
                            // detected it was offline before. Therefore it must
                            // have come online very recently and it is very
                            // unlikely that it will appear to be offline again
                            // before we get the URL. We simply try to verify
                            // the server's online status again.
                            if (!input) {
                                operation.serverBaseUrl = operation.clusterManagementService
                                        .getInterNodeCommunicationUrl(
                                                operation.serverId);
                                if (operation.serverBaseUrl == null) {
                                    return Futures.transform(
                                            operation.immediateFutureForThis,
                                            VERIFY_SERVER_OFFLINE);
                                }
                            }
                            return operation.immediateFutureForThis;
                        }
                    });
        }
    };

    // This function starts the update operation. It gets the channel
    // information (if not present yet) and configuration and delegates to
    // UPDATE_CHANNEL_STEP1.
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL = compose(
            ifThen(NEED_CHANNEL_INFORMATION, GET_CHANNEL_INFORMATION),
            GET_CHANNEL_CONFIGURATION, UPDATE_CHANNEL_STEP1);

    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP6 = compose(
            DELETE_PENDING_OPERATION, REFRESH_CHANNEL);

    // This function is called if the channel's control-system type does not
    // match the expected control-system type.
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP4A = compose(
            DELETE_PENDING_OPERATION, THROW_WRONG_CONTROL_SYSTEM_TYPE);

    // This function is called if the channel was moved to a different server
    // between getting the initial channel information and registering the
    // pending operation. This simply means that we have to start again from the
    // beginning.
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP4B = compose(
            DELETE_PENDING_OPERATION, UPDATE_CHANNEL);

    // This function is called if the updated retention periods would lead to an
    // invalid configuration (where a decimation level has a retention period
    // that is less than the one of the preceding decimation level).
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP4C = compose(
            DELETE_PENDING_OPERATION, THROW_VERIFY_RETENTION_PERIODS_EXCEPTION);

    // This function is called if the channel's server is offline. We have to
    // verify that the server is still offline after creating the pending
    // operation because it might have gone online in between. If it is online
    // now, we delegate to the server.
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP4D = compose(
            VERIFY_SERVER_OFFLINE,
            ifThenElse(VERIFIED_SERVER_OFFLINE,
                    ifThenElse(CONTROL_SYSTEM_SUPPORT_MISSING,
                            compose(DELETE_PENDING_OPERATION,
                                    THROW_CONTROL_SYSTEM_SUPPORT_MISSING),
                            compose(PREPARE_CREATE_DELETE_DECIMATION_LEVELS,
                                    THROW_IF_TIMEOUT, DELETE_SAMPLE_BUCKETS,
                                    THROW_IF_TIMEOUT, CREATE_DECIMATION_LEVELS,
                                    THROW_IF_TIMEOUT, DELETE_DECIMATION_LEVELS,
                                    THROW_IF_TIMEOUT, UPDATE_CHANNEL_STEP5)),
                    compose(DELETE_PENDING_OPERATION, FORWARD_TO_SERVER)));

    // This function is called if the channel is local. This is the regular
    // (expected) case.
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP4E = ifThenElse(
            CONTROL_SYSTEM_SUPPORT_MISSING,
            compose(DELETE_PENDING_OPERATION,
                    THROW_CONTROL_SYSTEM_SUPPORT_MISSING),
            compose(REFRESH_CHANNEL, PREPARE_CREATE_DELETE_DECIMATION_LEVELS,
                    THROW_IF_TIMEOUT, DELETE_SAMPLE_BUCKETS, THROW_IF_TIMEOUT,
                    CREATE_DECIMATION_LEVELS, THROW_IF_TIMEOUT,
                    DELETE_DECIMATION_LEVELS, THROW_IF_TIMEOUT,
                    UPDATE_CHANNEL_STEP5));

    // We check whether the control-system support for the channel is available
    // before registering the pending operation. This way, we do not have to
    // register a pending operation if it is not available. We have to do the
    // check again after registering the pending operation because in theory,
    // the control-system support could have changed in the meantime.
    private static final AsyncFunction<UpdateChannelOperation, UpdateChannelOperation> UPDATE_CHANNEL_STEP2 = ifThenElse(
            CONTROL_SYSTEM_SUPPORT_MISSING,
            THROW_CONTROL_SYSTEM_SUPPORT_MISSING,
            compose(CREATE_PENDING_OPERATION,
                    ifThenElse(CREATED_PENDING_OPERATION,
                            compose(GET_CHANNEL_CONFIGURATION,
                                    UPDATE_CHANNEL_STEP3),
                            THROW_PENDING_OPERATION_EXCEPTION)));

    private Set<Integer> addDecimationLevels;
    private Map<String, String> addOptions;
    private ArchivingService archivingService;
    private ChannelConfiguration channelConfiguration;
    private ChannelInformation channelInformation;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private String channelName;
    private ClusterManagementService clusterManagementService;
    private UpdateChannelCommand command;
    private boolean configurationModified;
    private ControlSystemSupport<?> controlSystemSupport;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private String controlSystemType;
    private boolean createdPendingOperation;
    private Set<Integer> decimationLevels;
    private Map<Integer, Integer> decimationLevelToRetentionPeriod;
    private Boolean enabled;
    private Executor executor;
    private String expectedControlSystemType;
    private UUID expectedServerId;
    private final ListenableFuture<UpdateChannelOperation> immediateFutureForThis = Futures
            .immediateFuture(this);
    private InterNodeCommunicationService interNodeCommunicationService;
    private boolean modified;
    private boolean needChannelInformation = true;
    private UUID operationId;
    private Map<String, String> options;
    private long pendingOperationCreatedTime;
    private final AsyncFunction<Object, UpdateChannelOperation> returnThis = new AsyncFunction<Object, UpdateChannelOperation>() {
        @Override
        public ListenableFuture<UpdateChannelOperation> apply(Object input)
                throws Exception {
            return immediateFutureForThis;
        }
    };
    private Set<Integer> removeDecimationLevels;
    private Set<String> removeOptions;
    private String serverBaseUrl;
    private UUID serverId;
    private UUID thisServerId;
    private boolean verifiedServerOffline;
    private RuntimeException verifyRetentionPeriodsException;

    private UpdateChannelOperation(ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            ControlSystemSupportRegistry controlSystemSupportRegistry,
            Executor executor,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, UpdateChannelCommand command) {
        this.archivingService = archivingService;
        this.channelInformationCache = channelInformationCache;
        this.channelMetaDataDAO = channelMetaDataDAO;
        this.clusterManagementService = clusterManagementService;
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
        this.executor = executor;
        this.interNodeCommunicationService = interNodeCommunicationService;
        this.thisServerId = thisServerId;
        // The command object already performs validation of its parameters
        // during construction. Therefore, we can use all the values without any
        // additional checks.
        this.command = command;
        this.addDecimationLevels = command.getAddDecimationLevels();
        this.addOptions = command.getAddOptions();
        this.channelName = command.getChannelName();
        this.decimationLevels = command.getDecimationLevels();
        this.decimationLevelToRetentionPeriod = command
                .getDecimationLevelToRetentionPeriod();
        this.enabled = command.getEnabled();
        this.expectedControlSystemType = command.getExpectedControlSystemType();
        this.expectedServerId = command.getExpectedServerId();
        this.options = command.getOptions();
        this.removeDecimationLevels = command.getRemoveDecimationLevels();
        this.removeOptions = command.getRemoveOptions();
        // We initialize the maps and sets with elements to be added or removed
        // if they are null. This way, we can easily use them with some
        // operations (e.g. intersect) that would not work with null values.
        if (this.addDecimationLevels == null) {
            this.addDecimationLevels = Collections.emptySet();
        }
        if (this.removeDecimationLevels == null) {
            this.removeDecimationLevels = Collections.emptySet();
        }
        if (this.addOptions == null) {
            this.addOptions = Collections.emptyMap();
        }
        if (this.removeOptions == null) {
            this.removeOptions = Collections.emptySet();
        }
    }

    /**
     * Updates a channel's configuration. The operation is performed in an
     * asynchronous way. The result is indicated through the returned future. In
     * case of error, the returned future throws an exception.
     * 
     * @param archivingService
     *            archiving service for this server. This is needed to refresh
     *            the channel locally.
     * @param channelInformationCache
     *            channel information cache that is used when an updated
     *            {@link ChannelInformation} needs to be retrieved. This way,
     *            the cache will be automatically updated with the most
     *            up-to-date information.
     * @param channelMetaDataDAO
     *            channel meta-data DAO for accessing the database.
     * @param clusterManagementService
     *            cluster management service. This is needed in order to
     *            determine the online status of remote servers.
     * @param controlSystemSupportRegistry
     *            control-system support registry for this server. The operation
     *            needs access to control-system supports because it has to
     *            delete all sample buckets for a decimation level before it can
     *            remove the decimation level from the channel.
     * @param executor
     *            executor used to run long running operations. Most operations
     *            are run using the same-thread executor. Some operations,
     *            however, that are related to deleting sample buckets might
     *            take some time when iterating over large amounts of data.
     *            Therefore, this operations are run using the specified
     *            executor so that they do not block an unpredictable and
     *            potentially unsafe thread.
     * @param interNodeCommunicationService
     *            inter-node communication service. This is needed in order to
     *            delegate the operation to a remote server if needed.
     * @param thisServerId
     *            ID of this server. This is needed to identify whether the
     *            operation is supposed to be performed locally or should be
     *            delegated to a remote server.
     * @param command
     *            update-channel command that shall be executed as part of this
     *            operation.
     * @param prefetchedChannelInformation
     *            reasonably up-to-date channel information for the specified
     *            channel. This information is used to determine whether the
     *            channel exists. If the channel does not exist (the channel
     *            information is <code>null</code>) the operation fails
     *            immediately.
     * @return future through which the completion of the operation can be
     *         monitored. If the operation resulted in the channel being
     *         modified, the future returns <code>true</code>. If the channel
     *         was not modified, the future returns <code>false</code>. Please
     *         note that the return value only indicates whether the operation
     *         modified the channel directly. If it was forwarded to a remote
     *         server and that server modified the channel, the future will
     *         still return <code>false</code>.
     */
    public static ListenableFuture<Boolean> updateChannel(
            ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            ControlSystemSupportRegistry controlSystemSupportRegistry,
            Executor executor,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, UpdateChannelCommand command,
            ChannelInformation prefetchedChannelInformation) {
        UpdateChannelOperation operation = new UpdateChannelOperation(
                archivingService, channelInformationCache, channelMetaDataDAO,
                clusterManagementService, controlSystemSupportRegistry,
                executor, interNodeCommunicationService, thisServerId, command);
        operation.channelInformation = prefetchedChannelInformation;
        operation.needChannelInformation = false;
        try {
            return Futures.transform(UPDATE_CHANNEL.apply(operation),
                    IS_MODIFIED);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
