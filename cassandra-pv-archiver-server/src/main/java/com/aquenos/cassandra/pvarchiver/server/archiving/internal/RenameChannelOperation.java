/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.aggregate;
import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.compose;
import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.ifThen;
import static com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils.ifThenElse;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelAlreadyExistsException;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.NoSuchChannelException;
import com.aquenos.cassandra.pvarchiver.server.archiving.PendingChannelOperationException;
import com.aquenos.cassandra.pvarchiver.server.archiving.RefreshChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.RenameChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Asynchronous operation for renaming a channel. The operation is encapsulated
 * in a class because it consists of many steps that are chained in an
 * asynchronous fashion in order to avoid blocking a thread for an extended
 * amount of time. This class is only intended for use by the
 * {@link ArchiveConfigurationService}.
 * 
 * @author Sebastian Marsching
 */
public final class RenameChannelOperation {

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> CREATE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.newChannelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_RENAME,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, RenameChannelOperation>() {
                                @Override
                                public ListenableFuture<RenameChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdGlobalPendingOperationForNewChannel = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> CREATE_LOCAL_PENDING_OPERATION_FOR_NEW_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            operation.serverId,
                            operation.newChannelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_RENAME,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, RenameChannelOperation>() {
                                @Override
                                public ListenableFuture<RenameChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdLocalPendingOperationForNewChannel = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> CREATE_GLOBAL_PENDING_OPERATION_FOR_OLD_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.oldChannelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_RENAME,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, RenameChannelOperation>() {
                                @Override
                                public ListenableFuture<RenameChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdGlobalPendingOperationForOldChannel = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> CREATE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            operation.pendingOperationCreatedTime = System.currentTimeMillis();
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            operation.serverId,
                            operation.oldChannelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_RENAME,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, RenameChannelOperation>() {
                                @Override
                                public ListenableFuture<RenameChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdLocalPendingOperationForOldChannel = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final Predicate<RenameChannelOperation> CREATED_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL = new Predicate<RenameChannelOperation>() {
        @Override
        public boolean apply(RenameChannelOperation operation) {
            return operation.createdGlobalPendingOperationForNewChannel;
        }
    };

    private static final Predicate<RenameChannelOperation> CREATED_LOCAL_PENDING_OPERATION_FOR_NEW_CHANNEL = new Predicate<RenameChannelOperation>() {
        @Override
        public boolean apply(RenameChannelOperation operation) {
            return operation.createdLocalPendingOperationForNewChannel;
        }
    };

    private static final Predicate<RenameChannelOperation> CREATED_GLOBAL_PENDING_OPERATION_FOR_OLD_CHANNEL = new Predicate<RenameChannelOperation>() {
        @Override
        public boolean apply(RenameChannelOperation operation) {
            return operation.createdGlobalPendingOperationForOldChannel;
        }
    };

    private static final Predicate<RenameChannelOperation> CREATED_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL = new Predicate<RenameChannelOperation>() {
        @Override
        public boolean apply(RenameChannelOperation operation) {
            return operation.createdLocalPendingOperationForOldChannel;
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> DELETE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // We can always delete the global pending operation. Another server
            // will see the channel that has been created and thus check for a
            // local pending operation.
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .deletePendingChannelOperation(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.newChannelName, operation.operationId);
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> DELETE_LOCAL_PENDING_OPERATION_FOR_NEW_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // If this operation did not run on the server that owns the
            // channel, we do not simply delete the pending operation but
            // replace it with a short-lived pending operation. This ensures
            // that the channel is not modified by a different server shortly
            // after creating it. Such a modification could be critical if there
            // is a clock skew (the modification might have a smaller time stamp
            // than the time stamp associated with our modifications and thus
            // the later modifications might get discarded. If the channel was
            // not modified we obviously can skip this. We also can skip it if
            // the operation ran on the correct server and the server is online
            // because in this case future operations will either run on the
            // same server (thus using the same clock) or will be delayed until
            // the server is considered "offline".
            ListenableFuture<Pair<Boolean, UUID>> future;
            if (operation.modified
                    && (!operation.thisServerId.equals(operation.serverId) || !operation.clusterManagementService
                            .isOnline())) {
                future = operation.channelMetaDataDAO
                        .updatePendingChannelOperation(
                                operation.serverId,
                                operation.newChannelName,
                                operation.operationId,
                                UUID.randomUUID(),
                                PendingChannelOperationConstants.OPERATION_PROTECTIVE,
                                "{ serverId: \"" + operation.thisServerId
                                        + "\", currentSystemTime: \""
                                        + System.currentTimeMillis() + "\" }",
                                PendingChannelOperationConstants.PROTECTIVE_PENDING_OPERATION_TTL_SECONDS);
            } else {
                future = operation.channelMetaDataDAO
                        .deletePendingChannelOperation(operation.serverId,
                                operation.newChannelName, operation.operationId);
            }
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> DELETE_GLOBAL_PENDING_OPERATION_FOR_OLD_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // We cannot simply delete the global pending operation for the old
            // channel. If another server that has a skewed clock created the
            // channel shortly after we deleted it, the delete could take
            // precedence over the create, resulting in vital information being
            // discarded in the database. Therefore, we use a protective pending
            // operation that will prevent the channel from being created again
            // for a few seconds.
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .updatePendingChannelOperation(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.oldChannelName,
                            operation.operationId,
                            UUID.randomUUID(),
                            PendingChannelOperationConstants.OPERATION_PROTECTIVE,
                            "{ serverId: \"" + operation.thisServerId
                                    + "\", currentSystemTime: \""
                                    + System.currentTimeMillis() + "\" }",
                            PendingChannelOperationConstants.PROTECTIVE_PENDING_OPERATION_TTL_SECONDS);
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // We can always delete the local pending operation for the old
            // channel. The channel does not exist any longer, so modifications
            // are not possible. The channel cannot be created again too early
            // because we still have the global pending operation.
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .deletePendingChannelOperation(operation.serverId,
                            operation.oldChannelName, operation.operationId);
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> DELETE_PENDING_OPERATIONS = compose(
            aggregate(DELETE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                    DELETE_GLOBAL_PENDING_OPERATION_FOR_OLD_CHANNEL,
                    DELETE_LOCAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                    DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL),
            new Function<List<RenameChannelOperation>, RenameChannelOperation>() {
                @Override
                public RenameChannelOperation apply(
                        List<RenameChannelOperation> input) {
                    return input.get(0);
                }
            });

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> FORWARD_TO_SERVER = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            return Futures
                    .transform(
                            operation.interNodeCommunicationService
                                    .runArchiveConfigurationCommands(
                                            operation.serverBaseUrl,
                                            Collections
                                                    .singletonList(operation.command)),
                            new Function<List<ArchiveConfigurationCommandResult>, RenameChannelOperation>() {
                                @Override
                                public RenameChannelOperation apply(
                                        List<ArchiveConfigurationCommandResult> input) {
                                    assert (input.size() == 1);
                                    ArchiveConfigurationCommandResult result = input
                                            .get(0);
                                    if (result.isSuccess()) {
                                        return operation;
                                    } else {
                                        throw new RuntimeException(
                                                "Server "
                                                        + operation.serverId
                                                        + " reported an error when trying to rename channel \""
                                                        + StringEscapeUtils
                                                                .escapeJava(operation.oldChannelName)
                                                        + "\" to \""
                                                        + StringEscapeUtils
                                                                .escapeJava(operation.newChannelName)
                                                        + "\""
                                                        + (result
                                                                .getErrorMessage() != null ? ": "
                                                                + result.getErrorMessage()
                                                                : "."));
                                    }
                                }
                            });
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> GET_NEW_CHANNEL_INFORMATION = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            return Futures.transform(operation.channelInformationCache
                    .getChannelAsync(operation.newChannelName, true),
                    new Function<ChannelInformation, RenameChannelOperation>() {
                        @Override
                        public RenameChannelOperation apply(
                                ChannelInformation input) {
                            operation.newChannelInformation = input;
                            return operation;
                        }
                    });
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> GET_OLD_CHANNEL_INFORMATION = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            return Futures.transform(operation.channelInformationCache
                    .getChannelAsync(operation.oldChannelName, true),
                    new Function<ChannelInformation, RenameChannelOperation>() {
                        @Override
                        public RenameChannelOperation apply(
                                ChannelInformation input) {
                            operation.oldChannelInformation = input;
                            operation.needOldChannelInformation = false;
                            return operation;
                        }
                    });
        }
    };

    private static final Function<RenameChannelOperation, Boolean> IS_MODIFIED = new Function<RenameChannelOperation, Boolean>() {
        @Override
        public Boolean apply(RenameChannelOperation input) {
            return input.modified;
        }
    };

    private static final Predicate<RenameChannelOperation> NEED_OLD_CHANNEL_INFORMATION = new Predicate<RenameChannelOperation>() {
        @Override
        public boolean apply(RenameChannelOperation operation) {
            return operation.needOldChannelInformation;
        }
    };

    private static final Function<List<RenameChannelOperation>, RenameChannelOperation> PICK_FIRST_OPERATION = new Function<List<RenameChannelOperation>, RenameChannelOperation>() {
        @Override
        public RenameChannelOperation apply(List<RenameChannelOperation> input) {
            if (input.isEmpty()) {
                return null;
            } else {
                return input.get(0);
            }
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> PREPARE_PENDING_OPERATION = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            operation.operationId = UUID.randomUUID();
            StringBuilder operationDataBuilder = new StringBuilder();
            operationDataBuilder.append("{ \"oldChannelName\": \"");
            operationDataBuilder.append(StringEscapeUtils
                    .escapeJson(operation.oldChannelName));
            operationDataBuilder.append("\", \"newChannelName\": ");
            operationDataBuilder.append(StringEscapeUtils
                    .escapeJson(operation.newChannelName));
            operationDataBuilder.append("\" }");
            operation.operationData = operationDataBuilder.toString();
            return operation.immediateFutureForThis;
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> REFRESH_NEW_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            if (operation.thisServerId.equals(operation.serverId)) {
                return Futures.transform(operation.archivingService
                        .refreshChannel(operation.newChannelName),
                        operation.returnThis);
            } else {
                String targetServerBaseUrl = operation.clusterManagementService
                        .getInterNodeCommunicationUrl(operation.serverId);
                if (targetServerBaseUrl != null) {
                    return Futures
                            .transform(
                                    operation.interNodeCommunicationService
                                            .runArchiveConfigurationCommands(
                                                    targetServerBaseUrl,
                                                    Collections
                                                            .singletonList(new RefreshChannelCommand(
                                                                    operation.newChannelName,
                                                                    operation.serverId))),
                                    operation.returnThis);
                } else {
                    return operation.immediateFutureForThis;
                }
            }
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> REFRESH_OLD_CHANNEL = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            if (operation.thisServerId.equals(operation.serverId)) {
                return Futures.transform(operation.archivingService
                        .refreshChannel(operation.oldChannelName),
                        operation.returnThis);
            } else {
                String targetServerBaseUrl = operation.clusterManagementService
                        .getInterNodeCommunicationUrl(operation.serverId);
                if (targetServerBaseUrl != null) {
                    return Futures
                            .transform(
                                    operation.interNodeCommunicationService
                                            .runArchiveConfigurationCommands(
                                                    targetServerBaseUrl,
                                                    Collections
                                                            .singletonList(new RefreshChannelCommand(
                                                                    operation.oldChannelName,
                                                                    operation.serverId))),
                                    operation.returnThis);
                } else {
                    return operation.immediateFutureForThis;
                }
            }
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP1 = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // We can only rename a channel that already exists, so if we did
            // not get the channel information, we throw an exception.
            if (operation.oldChannelInformation == null) {
                return THROW_NO_SUCH_CHANNEL_EXCEPTION.apply(operation);
            }
            // If the old and the new name are the same, we do not have to do
            // anything.
            if (operation.oldChannelName.equals(operation.newChannelName)) {
                return operation.immediateFutureForThis;
            }
            // We cannot use a channel name that already exists, so if we found
            // any channel information for the new name, we throw an exception.
            if (operation.newChannelInformation != null) {
                return THROW_CHANNEL_ALREADY_EXISTS_EXCEPTION.apply(operation);
            }
            operation.serverId = operation.oldChannelInformation.getServerId();
            if (operation.expectedServerId != null
                    && !operation.expectedServerId.equals(operation.serverId)) {
                throw new IllegalArgumentException(
                        "The channel \""
                                + StringEscapeUtils
                                        .escapeJava(operation.oldChannelName)
                                + "\" is registered with the server "
                                + operation.serverId + ", but the server "
                                + operation.expectedServerId
                                + " was specified.");
            }
            operation.serverBaseUrl = operation.clusterManagementService
                    .getInterNodeCommunicationUrl(operation.serverId);
            if (!operation.thisServerId.equals(operation.serverId)
                    && operation.serverBaseUrl != null) {
                return FORWARD_TO_SERVER.apply(operation);
            } else {
                return RENAME_CHANNEL_STEP2.apply(operation);
            }
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP3 = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // The channel configuration might have changed since we got it the
            // last time (before creating the pending operation). Therefore, we
            // basically have to run all checks again.
            // We can only rename a channel that already exists, so if we did
            // not get the channel information, we throw an exception. However,
            // we first remove the pending operations.
            if (operation.oldChannelInformation == null) {
                return RENAME_CHANNEL_STEP4A.apply(operation);
            }
            // We cannot use a channel name that already exists, so if we found
            // any channel information for the new name, we throw an exception.
            // However, we first remove the pending operations.
            if (operation.newChannelInformation != null) {
                return RENAME_CHANNEL_STEP4B.apply(operation);
            }
            // If the server that owns the channel changed, we registered the
            // pending operation for the old server. We have to delete the
            // pending operation and start again from the beginning.
            if (!operation.serverId.equals(operation.oldChannelInformation
                    .getServerId())) {
                return RENAME_CHANNEL_STEP4C.apply(operation);
            }
            // If the channel is not registered with this server, we have to
            // make sure that the owning server is still offline.
            if (!operation.thisServerId.equals(operation.serverId)) {
                return RENAME_CHANNEL_STEP4D.apply(operation);
            } else {
                return RENAME_CHANNEL_STEP4E.apply(operation);
            }
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP6 = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            operation.modified = true;
            return Futures.transform(Futures.transform(
                    operation.channelMetaDataDAO.renameChannel(
                            operation.oldChannelName, operation.newChannelName,
                            operation.serverId), operation.returnThis),
                    RENAME_CHANNEL_STEP7);
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> THROW_CHANNEL_ALREADY_EXISTS_EXCEPTION = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            throw new ChannelAlreadyExistsException("Channel \""
                    + StringEscapeUtils.escapeJava(operation.oldChannelName)
                    + "\" cannot be renamed to \""
                    + StringEscapeUtils.escapeJava(operation.newChannelName)
                    + "\" because a channel with the same name already exists.");
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> THROW_IF_TIMEOUT = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            // If more than two minutes have passed since creating the pending
            // operation, something is going wrong. If we continued, we would
            // risk making changes after the pending operation has expired, so
            // we rather throw an exception.
            if (System.currentTimeMillis() > operation.pendingOperationCreatedTime + 120000L) {
                throw new RuntimeException(
                        "Channel \""
                                + StringEscapeUtils
                                        .escapeJava(operation.oldChannelName)
                                + "\" cannot be renamed because the operation took too long to finish.");
            } else {
                return operation.immediateFutureForThis;
            }
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> THROW_NO_SUCH_CHANNEL_EXCEPTION = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            throw new NoSuchChannelException("Channel \""
                    + StringEscapeUtils.escapeJava(operation.oldChannelName)
                    + "\" cannot be renamed to \""
                    + StringEscapeUtils.escapeJava(operation.newChannelName)
                    + "\" because the channel does not exist.");
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> THROW_PENDING_OPERATION_EXCEPTION = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            throw new PendingChannelOperationException(
                    "Channel \""
                            + StringEscapeUtils
                                    .escapeJava(operation.oldChannelName)
                            + "\" cannot be renamed to \""
                            + StringEscapeUtils
                                    .escapeJava(operation.newChannelName)
                            + "\" because another operation for this channel is pending.");
        }
    };

    private static final Predicate<RenameChannelOperation> VERIFIED_SERVER_OFFLINE = new Predicate<RenameChannelOperation>() {
        @Override
        public boolean apply(RenameChannelOperation operation) {
            return operation.verifiedServerOffline;
        }
    };

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> VERIFY_SERVER_OFFLINE = new AsyncFunction<RenameChannelOperation, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(
                final RenameChannelOperation operation) throws Exception {
            return Futures.transform(operation.clusterManagementService
                    .verifyServerOffline(operation.serverId),
                    new AsyncFunction<Boolean, RenameChannelOperation>() {
                        @Override
                        public ListenableFuture<RenameChannelOperation> apply(
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
                                        .getInterNodeCommunicationUrl(operation.serverId);
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

    // This function starts the rename operation. It only gets the channel
    // information for both the old and new channel name and delegates to
    // RENAME_CHANNEL_STEP1. We need both pieces of information because we
    // cannot rename the channel if it does not exist under the older name and
    // we cannot rename it either if another channel with the new name already
    // exists.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL = compose(
            compose(aggregate(
                    GET_NEW_CHANNEL_INFORMATION,
                    ifThen(NEED_OLD_CHANNEL_INFORMATION,
                            GET_OLD_CHANNEL_INFORMATION)), PICK_FIRST_OPERATION),
            RENAME_CHANNEL_STEP1);

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP7 = compose(
            DELETE_PENDING_OPERATIONS, REFRESH_OLD_CHANNEL, REFRESH_NEW_CHANNEL);

    // Please note that for renaming a channel a total of four (!) pending
    // operations is needed. The global (unassigned server ID) one for the old
    // name ensures that no other server will try to add a channel with the old
    // name before the rename operation has finished. The global one for the new
    // name ensures that no other server will add a channel with the new name
    // before we are finished. The two local ones ensure that the server will
    // not use the channel under the old or new name while the rename operation
    // is in progress.
    // Here we assume that creating the two pending operation always succeeds:
    // We created two other pending operations earlier which should prevent any
    // concurrent operation from progressing to the point where it would
    // register the same pending operations. Therefore, we simply throw an
    // exception if there is a problem instead of deleting the pending
    // operations that were created earlier.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP5 = compose(
            compose(aggregate(CREATE_GLOBAL_PENDING_OPERATION_FOR_OLD_CHANNEL,
                    CREATE_LOCAL_PENDING_OPERATION_FOR_NEW_CHANNEL),
                    PICK_FIRST_OPERATION),
            ifThenElse(
                    Predicates.and(
                            CREATED_GLOBAL_PENDING_OPERATION_FOR_OLD_CHANNEL,
                            CREATED_LOCAL_PENDING_OPERATION_FOR_NEW_CHANNEL),
                    compose(THROW_IF_TIMEOUT, RENAME_CHANNEL_STEP6),
                    compose(DELETE_PENDING_OPERATIONS,
                            THROW_PENDING_OPERATION_EXCEPTION)));

    // This function is called when the channel does not exist any longer (at
    // least under the specified name) after the pending operations have been
    // registered. This only happens when the channel is removed or renamed
    // after we got the initial channel information but before we registered the
    // pending operation.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP4A = compose(
            compose(aggregate(DELETE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                    DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL),
                    PICK_FIRST_OPERATION), THROW_NO_SUCH_CHANNEL_EXCEPTION);

    // This function is called when the new name cannot be used because another
    // channel already exists. We check this before registering the pending
    // operations, but it is possible that a channel with that name is created
    // after we checked the channel information but before we registered the
    // pending operation.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP4B = compose(
            compose(aggregate(DELETE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                    DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL),
                    PICK_FIRST_OPERATION),
            THROW_CHANNEL_ALREADY_EXISTS_EXCEPTION);

    // This function is called if the channel is now registered with a different
    // server. This is not a problem, but we have to restart the process using
    // the right server.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP4C = compose(
            compose(aggregate(DELETE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                    DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL),
                    PICK_FIRST_OPERATION), RENAME_CHANNEL_STEP1);

    // This function is called if the channel is registered with a different
    // server than the one that is running the operation. In this case, we have
    // to verify that the actual server is still offline. If it is not, we have
    // to proceed by delegating the operation to that server.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP4D = compose(
            VERIFY_SERVER_OFFLINE,
            ifThenElse(
                    VERIFIED_SERVER_OFFLINE,
                    RENAME_CHANNEL_STEP5,
                    compose(compose(

                            aggregate(
                                    DELETE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                                    DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL),
                            PICK_FIRST_OPERATION), FORWARD_TO_SERVER)));

    // This function is called if the channel is registered with the local
    // server and we can proceed. This is the regular (expected) case.
    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP4E = compose(
            REFRESH_OLD_CHANNEL, RENAME_CHANNEL_STEP5);

    private static final AsyncFunction<RenameChannelOperation, RenameChannelOperation> RENAME_CHANNEL_STEP2 = compose(
            PREPARE_PENDING_OPERATION,
            CREATE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL,
            ifThenElse(
                    CREATED_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL,
                    compose(CREATE_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                            ifThenElse(
                                    CREATED_GLOBAL_PENDING_OPERATION_FOR_NEW_CHANNEL,
                                    compose(compose(
                                            aggregate(
                                                    GET_NEW_CHANNEL_INFORMATION,
                                                    GET_OLD_CHANNEL_INFORMATION),
                                            PICK_FIRST_OPERATION),
                                            RENAME_CHANNEL_STEP3),
                                    compose(DELETE_LOCAL_PENDING_OPERATION_FOR_OLD_CHANNEL,
                                            THROW_PENDING_OPERATION_EXCEPTION))),
                    THROW_PENDING_OPERATION_EXCEPTION));

    private ArchivingService archivingService;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ClusterManagementService clusterManagementService;
    private RenameChannelCommand command;
    private boolean createdGlobalPendingOperationForNewChannel;
    private boolean createdLocalPendingOperationForNewChannel;
    private boolean createdGlobalPendingOperationForOldChannel;
    private boolean createdLocalPendingOperationForOldChannel;
    private UUID expectedServerId;
    private final ListenableFuture<RenameChannelOperation> immediateFutureForThis = Futures
            .immediateFuture(this);
    private InterNodeCommunicationService interNodeCommunicationService;
    private boolean modified;
    private boolean needOldChannelInformation = true;
    private ChannelInformation newChannelInformation;
    private String newChannelName;
    private ChannelInformation oldChannelInformation;
    private String oldChannelName;
    private String operationData;
    private UUID operationId;
    private long pendingOperationCreatedTime;
    private final AsyncFunction<Object, RenameChannelOperation> returnThis = new AsyncFunction<Object, RenameChannelOperation>() {
        @Override
        public ListenableFuture<RenameChannelOperation> apply(Object input)
                throws Exception {
            return immediateFutureForThis;
        }
    };
    private String serverBaseUrl;
    private UUID serverId;
    private UUID thisServerId;
    private boolean verifiedServerOffline;

    private RenameChannelOperation(ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, RenameChannelCommand command) {
        this.archivingService = archivingService;
        this.channelInformationCache = channelInformationCache;
        this.channelMetaDataDAO = channelMetaDataDAO;
        this.clusterManagementService = clusterManagementService;
        this.interNodeCommunicationService = interNodeCommunicationService;
        this.thisServerId = thisServerId;
        // The command object already performs validation of its parameters
        // during construction. Therefore, we can use all the values without any
        // additional checks.
        this.command = command;
        this.newChannelName = command.getNewChannelName();
        this.oldChannelName = command.getOldChannelName();
        this.expectedServerId = command.getExpectedServerId();
    }

    /**
     * Renames a channel. The operation is performed in an asynchronous way. The
     * result is indicated through the returned future. In case of error, the
     * returned future throws an exception.
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
     * @param interNodeCommunicationService
     *            inter-node communication service. This is needed in order to
     *            delegate the operation to a remote server if needed.
     * @param thisServerId
     *            ID of this server. This is needed to identify whether the
     *            operation is supposed to be performed locally or should be
     *            delegated to a remote server.
     * @param command
     *            rename-channel command that shall be executed as part of this
     *            operation.
     * @param prefetchedChannelInformation
     *            reasonably up-to-date channel information for the old channel
     *            name. This information is used to determine whether the
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
    public static ListenableFuture<Boolean> renameChannel(
            ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, RenameChannelCommand command,
            ChannelInformation prefetchedChannelInformation) {
        RenameChannelOperation operation = new RenameChannelOperation(
                archivingService, channelInformationCache, channelMetaDataDAO,
                clusterManagementService, interNodeCommunicationService,
                thisServerId, command);
        operation.oldChannelInformation = prefetchedChannelInformation;
        operation.needOldChannelInformation = false;
        try {
            return Futures.transform(RENAME_CHANNEL.apply(operation),
                    IS_MODIFIED);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
