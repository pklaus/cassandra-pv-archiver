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
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.MoveChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.NoSuchChannelException;
import com.aquenos.cassandra.pvarchiver.server.archiving.PendingChannelOperationException;
import com.aquenos.cassandra.pvarchiver.server.archiving.RefreshChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Asynchronous operation for moving a channel. The operation is encapsulated in
 * a class because it consists of many steps that are chained in an asynchronous
 * fashion in order to avoid blocking a thread for an extended amount of time.
 * This class is only intended for use by the
 * {@link ArchiveConfigurationService}.
 * 
 * @author Sebastian Marsching
 */
public final class MoveChannelOperation {

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> CREATE_PENDING_OPERATION_NEW_SERVER = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            operation.newServerId,
                            operation.channelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_MOVE,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, MoveChannelOperation>() {
                                @Override
                                public ListenableFuture<MoveChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdPendingOperationForNewServer = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> CREATE_PENDING_OPERATION_OLD_SERVER = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            operation.pendingOperationCreatedTime = System.currentTimeMillis();
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            operation.oldServerId,
                            operation.channelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_MOVE,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, MoveChannelOperation>() {
                                @Override
                                public ListenableFuture<MoveChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdPendingOperationForOldServer = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final Predicate<MoveChannelOperation> CREATED_PENDING_OPERATION_NEW_SERVER = new Predicate<MoveChannelOperation>() {
        @Override
        public boolean apply(MoveChannelOperation operation) {
            return operation.createdPendingOperationForNewServer;
        }
    };

    private static final Predicate<MoveChannelOperation> CREATED_PENDING_OPERATION_OLD_SERVER = new Predicate<MoveChannelOperation>() {
        @Override
        public boolean apply(MoveChannelOperation operation) {
            return operation.createdPendingOperationForOldServer;
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> DELETE_PENDING_OPERATION_NEW_SERVER = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            // If this operation did not run on the server that now owns the
            // channel, we do not simply delete the pending operation but
            // replace it with a short-lived pending operation. This ensures
            // that the channel is not modified by a different server shortly
            // after modifying it. Such a modification could be critical if
            // there is a clock skew (the modification might have a smaller time
            // stamp than the time stamp associated with our modifications and
            // thus the later modifications might get discarded. If the channel
            // was not modified, we obviously can skip this. We also can skip it
            // if the operation ran on the correct server and the server is
            // online because in this case future operations will either run on
            // the same server (thus using the same clock) or will be delayed
            // until the server is considered "offline".
            ListenableFuture<Pair<Boolean, UUID>> future;
            if (operation.modified
                    && (!operation.thisServerId.equals(operation.newServerId) || !operation.clusterManagementService
                            .isOnline())) {
                future = operation.channelMetaDataDAO
                        .updatePendingChannelOperation(
                                operation.newServerId,
                                operation.channelName,
                                operation.operationId,
                                UUID.randomUUID(),
                                PendingChannelOperationConstants.OPERATION_PROTECTIVE,
                                "{ serverId: \"" + operation.thisServerId
                                        + "\", currentSystemTime: \""
                                        + System.currentTimeMillis() + "\" }",
                                PendingChannelOperationConstants.PROTECTIVE_PENDING_OPERATION_TTL_SECONDS);
            } else {
                future = operation.channelMetaDataDAO
                        .deletePendingChannelOperation(operation.newServerId,
                                operation.channelName, operation.operationId);
            }
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> DELETE_PENDING_OPERATION_OLD_SERVER = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            // We can always delete the pending operation for the old server.
            // Typically, a move operation is run on the old server because this
            // is the only way to ensure that earlier modifications will not
            // take precedence over the move operation. The only situation in
            // which the operation is run on a different server is when the old
            // server is offline. In this case the operation is typically run on
            // the new server and future modifications will also run on the new
            // server. In case both servers are offline, the protective pending
            // operation created for the new server will prevent further
            // modifications that happen to soon.
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .deletePendingChannelOperation(operation.oldServerId,
                            operation.channelName, operation.operationId);
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> DELETE_PENDING_OPERATIONS = compose(
            aggregate(DELETE_PENDING_OPERATION_NEW_SERVER,
                    DELETE_PENDING_OPERATION_OLD_SERVER),
            new Function<List<MoveChannelOperation>, MoveChannelOperation>() {
                @Override
                public MoveChannelOperation apply(
                        List<MoveChannelOperation> input) {
                    return input.get(0);
                }
            });

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> FORWARD_TO_OLD_SERVER = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            return Futures
                    .transform(
                            operation.interNodeCommunicationService
                                    .runArchiveConfigurationCommands(
                                            operation.oldServerBaseUrl,
                                            Collections
                                                    .singletonList(operation.command)),
                            new Function<List<ArchiveConfigurationCommandResult>, MoveChannelOperation>() {
                                @Override
                                public MoveChannelOperation apply(
                                        List<ArchiveConfigurationCommandResult> input) {
                                    assert (input.size() == 1);
                                    ArchiveConfigurationCommandResult result = input
                                            .get(0);
                                    if (result.isSuccess()) {
                                        return operation;
                                    } else {
                                        throw new RuntimeException(
                                                "Server "
                                                        + operation.oldServerId
                                                        + " reported an error when trying to move channel \""
                                                        + StringEscapeUtils
                                                                .escapeJava(operation.channelName)
                                                        + "\" to server "
                                                        + operation.newServerId
                                                        + (result
                                                                .getErrorMessage() != null ? ": "
                                                                + result.getErrorMessage()
                                                                : "."));
                                    }
                                }
                            });
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> GET_CHANNEL_INFORMATION = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            return Futures.transform(operation.channelInformationCache
                    .getChannelAsync(operation.channelName, true),
                    new Function<ChannelInformation, MoveChannelOperation>() {
                        @Override
                        public MoveChannelOperation apply(
                                ChannelInformation input) {
                            operation.channelInformation = input;
                            operation.needChannelInformation = false;
                            return operation;
                        }
                    });
        }
    };

    private static final Function<MoveChannelOperation, Boolean> IS_MODIFIED = new Function<MoveChannelOperation, Boolean>() {
        @Override
        public Boolean apply(MoveChannelOperation input) {
            return input.modified;
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> PREPARE_PENDING_OPERATION = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            operation.operationId = UUID.randomUUID();
            StringBuilder operationDataBuilder = new StringBuilder();
            operationDataBuilder.append("{ \"expectedOldServerId\": \"");
            operationDataBuilder.append(operation.oldServerId.toString());
            operationDataBuilder.append("\", \"newServerId\": ");
            operationDataBuilder.append(operation.newServerId.toString());
            operationDataBuilder.append("\" }");
            operation.operationData = operationDataBuilder.toString();
            return operation.immediateFutureForThis;
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP1 = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            // We can only move a channel that already exists, so if we did not
            // get the channel information, we throw an exception.
            if (operation.channelInformation == null) {
                return THROW_NO_SUCH_CHANNEL_EXCEPTION.apply(operation);
            }
            operation.oldServerId = operation.channelInformation.getServerId();
            // If the old and the new server are identical, we do not have to
            // move the channel at all.
            if (operation.oldServerId.equals(operation.newServerId)) {
                return operation.immediateFutureForThis;
            }
            if (operation.expectedOldServerId != null
                    && !operation.expectedOldServerId
                            .equals(operation.oldServerId)) {
                throw new IllegalArgumentException("The channel \""
                        + StringEscapeUtils.escapeJava(operation.channelName)
                        + "\" is registered with the server "
                        + operation.oldServerId + ", but the server "
                        + operation.expectedOldServerId + " was specified.");
            }
            operation.oldServerBaseUrl = operation.clusterManagementService
                    .getInterNodeCommunicationUrl(operation.oldServerId);
            if (!operation.thisServerId.equals(operation.oldServerId)
                    && operation.oldServerBaseUrl != null) {
                return FORWARD_TO_OLD_SERVER.apply(operation);
            } else {
                return MOVE_CHANNEL_STEP2.apply(operation);
            }
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP3 = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {

        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            // The channel configuration might have changed since we got it the
            // last time (before creating the pending operation). Therefore, we
            // basically have to run all checks again.
            // We can only move a channel that already exists, so if we did not
            // get the channel information, we throw an exception. However, we
            // first remove the pending operation.
            if (operation.channelInformation == null) {
                return MOVE_CHANNEL_STEP4A.apply(operation);
            }
            // If the server that owns the channel changed, we registered the
            // pending operation for the old server. We have to delete the
            // pending operation and start again from the beginning.
            if (!operation.oldServerId.equals(operation.channelInformation
                    .getServerId())) {
                return MOVE_CHANNEL_STEP4B.apply(operation);
            }
            // If the channel is not registered with this server, we have to
            // make sure that the old server is still offline.
            if (!operation.thisServerId.equals(operation.oldServerId)) {
                return MOVE_CHANNEL_STEP4C.apply(operation);
            } else {
                return MOVE_CHANNEL_STEP4D.apply(operation);
            }
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP6 = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            operation.modified = true;
            return Futures.transform(Futures.transform(
                    operation.channelMetaDataDAO.moveChannel(
                            operation.channelName, operation.oldServerId,
                            operation.newServerId), operation.returnThis),
                    MOVE_CHANNEL_STEP7);
        }
    };

    private static final Predicate<MoveChannelOperation> NEED_CHANNEL_INFORMATION = new Predicate<MoveChannelOperation>() {
        @Override
        public boolean apply(MoveChannelOperation operation) {
            return operation.needChannelInformation;
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> REFRESH_NEW_CHANNEL = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            if (operation.thisServerId.equals(operation.newServerId)) {
                return Futures.transform(operation.archivingService
                        .refreshChannel(operation.channelName),
                        operation.returnThis);
            } else {
                String newServerBaseUrl = operation.clusterManagementService
                        .getInterNodeCommunicationUrl(operation.newServerId);
                if (newServerBaseUrl != null) {
                    Futures.transform(
                            operation.interNodeCommunicationService.runArchiveConfigurationCommands(
                                    newServerBaseUrl,
                                    Collections
                                            .singletonList(new RefreshChannelCommand(
                                                    operation.channelName,
                                                    operation.newServerId))),
                            new Function<List<ArchiveConfigurationCommandResult>, MoveChannelOperation>() {
                                @Override
                                public MoveChannelOperation apply(
                                        List<ArchiveConfigurationCommandResult> input) {
                                    assert (input.size() == 1);
                                    ArchiveConfigurationCommandResult result = input
                                            .get(0);
                                    if (result.isSuccess()) {
                                        return operation;
                                    } else {
                                        throw new RuntimeException(
                                                "Server "
                                                        + operation.newServerId
                                                        + " reported an error when trying to refresh the channel \""
                                                        + StringEscapeUtils
                                                                .escapeJava(operation.channelName)
                                                        + "\""
                                                        + (result
                                                                .getErrorMessage() != null ? ": "
                                                                + result.getErrorMessage()
                                                                : "."));
                                    }
                                }
                            });
                    return operation.immediateFutureForThis;
                } else {
                    return operation.immediateFutureForThis;
                }

            }
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> REFRESH_OLD_CHANNEL_IF_LOCAL = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            if (operation.thisServerId.equals(operation.oldServerId)) {
                return Futures.transform(operation.archivingService
                        .refreshChannel(operation.channelName),
                        operation.returnThis);
            } else {
                return operation.immediateFutureForThis;
            }
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> THROW_IF_TIMEOUT = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            // If more than two minutes have passed since creating the pending
            // operation, something is going wrong. If we continued, we would
            // risk making changes after the pending operation has expired, so
            // we rather throw an exception.
            if (System.currentTimeMillis() > operation.pendingOperationCreatedTime + 120000L) {
                throw new RuntimeException(
                        "Channel \""
                                + StringEscapeUtils
                                        .escapeJava(operation.channelName)
                                + "\" cannot be moved because the operation took too long to finish.");
            } else {
                return operation.immediateFutureForThis;
            }
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> THROW_NO_SUCH_CHANNEL_EXCEPTION = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            throw new NoSuchChannelException("Channel \""
                    + StringEscapeUtils.escapeJava(operation.channelName)
                    + "\" cannot be moved because it does not exist.");
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> THROW_PENDING_OPERATION_EXCEPTION = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            throw new PendingChannelOperationException(
                    "Channel \""
                            + StringEscapeUtils
                                    .escapeJava(operation.channelName)
                            + "\" cannot be moved because another operation for this channel is pending.");
        }
    };

    private static final Predicate<MoveChannelOperation> VERIFIED_OLD_SERVER_OFFLINE = new Predicate<MoveChannelOperation>() {
        @Override
        public boolean apply(MoveChannelOperation operation) {
            return operation.verifiedOldServerOffline;
        }
    };

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> VERIFY_OLD_SERVER_OFFLINE = new AsyncFunction<MoveChannelOperation, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(
                final MoveChannelOperation operation) throws Exception {
            return Futures.transform(operation.clusterManagementService
                    .verifyServerOffline(operation.oldServerId),
                    new AsyncFunction<Boolean, MoveChannelOperation>() {
                        @Override
                        public ListenableFuture<MoveChannelOperation> apply(
                                Boolean input) throws Exception {
                            operation.verifiedOldServerOffline = input;
                            // There is an inherent race condition here, but we
                            // only verify whether the server is offline if we
                            // detected it was offline before. Therefore it must
                            // have come online very recently and it is very
                            // unlikely that it will appear to be offline again
                            // before we get the URL. We simply try to verify
                            // the server's online status again.
                            if (!input) {
                                operation.oldServerBaseUrl = operation.clusterManagementService
                                        .getInterNodeCommunicationUrl(operation.oldServerId);
                                if (operation.oldServerBaseUrl == null) {
                                    return Futures.transform(
                                            operation.immediateFutureForThis,
                                            VERIFY_OLD_SERVER_OFFLINE);
                                }
                            }
                            return operation.immediateFutureForThis;
                        }
                    });
        }
    };

    // This is the function that starts the move channel operation. It only gets
    // the channel information (if not available yet) and delegates to
    // MOVE_CHANNEL_STEP1.
    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL = compose(
            ifThen(NEED_CHANNEL_INFORMATION, GET_CHANNEL_INFORMATION),
            MOVE_CHANNEL_STEP1);

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP7 = compose(
            DELETE_PENDING_OPERATIONS, REFRESH_OLD_CHANNEL_IF_LOCAL,
            REFRESH_NEW_CHANNEL);

    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP5 = compose(
            CREATE_PENDING_OPERATION_NEW_SERVER,
            ifThenElse(
                    CREATED_PENDING_OPERATION_NEW_SERVER,
                    compose(THROW_IF_TIMEOUT, MOVE_CHANNEL_STEP6),
                    compose(DELETE_PENDING_OPERATION_OLD_SERVER,
                            THROW_PENDING_OPERATION_EXCEPTION)));

    // This function is only called if the channel does not exist after
    // registering the pending operation. This means that it must have been
    // deleted after the operation had started but before we registered the
    // pending operation.
    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP4A = compose(
            DELETE_PENDING_OPERATION_OLD_SERVER,
            THROW_NO_SUCH_CHANNEL_EXCEPTION);

    // This function is only called if the channel was moved in the meantime.
    // This simply means that we have to restart the whole process by delegating
    // back to MOVE_CHANNEL.
    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP4B = compose(
            DELETE_PENDING_OPERATION_OLD_SERVER, MOVE_CHANNEL);

    // MOVE_CHANNEL_STEP4C and MOVE_CHANNEL_STEP4D only differ slightly. The
    // first one is used if we are moving a channel that belongs to a remote
    // server. In this case we have to verify that the server is actually
    // offline or delegate to the remote server if it is online.
    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP4C = compose(
            VERIFY_OLD_SERVER_OFFLINE,
            ifThenElse(
                    VERIFIED_OLD_SERVER_OFFLINE,
                    MOVE_CHANNEL_STEP5,
                    compose(DELETE_PENDING_OPERATION_NEW_SERVER,
                            FORWARD_TO_OLD_SERVER)));

    // This is the regular case (moving a channel that currently belongs to this
    // server).
    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP4D = compose(
            REFRESH_OLD_CHANNEL_IF_LOCAL, MOVE_CHANNEL_STEP5);

    // Please note that for moving a channel, two pending operations are needed.
    // The one for the old server ensures that the server does not use the
    // channel any longer while the move operation is in progress. The one for
    // the new server ensures that the new server will not use the channel
    // before the move operation has finished.
    private static final AsyncFunction<MoveChannelOperation, MoveChannelOperation> MOVE_CHANNEL_STEP2 = compose(
            PREPARE_PENDING_OPERATION,
            CREATE_PENDING_OPERATION_OLD_SERVER,
            ifThenElse(CREATED_PENDING_OPERATION_OLD_SERVER,
                    compose(GET_CHANNEL_INFORMATION, MOVE_CHANNEL_STEP3),
                    THROW_PENDING_OPERATION_EXCEPTION));

    private ArchivingService archivingService;
    private ChannelInformation channelInformation;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private String channelName;
    private ClusterManagementService clusterManagementService;
    private MoveChannelCommand command;
    private boolean createdPendingOperationForOldServer;
    private boolean createdPendingOperationForNewServer;
    private UUID expectedOldServerId;
    private final ListenableFuture<MoveChannelOperation> immediateFutureForThis = Futures
            .immediateFuture(this);
    private InterNodeCommunicationService interNodeCommunicationService;
    private boolean modified;
    private boolean needChannelInformation = true;
    private UUID newServerId;
    private String oldServerBaseUrl;
    private UUID oldServerId;
    private UUID operationId;
    private String operationData;
    private long pendingOperationCreatedTime;
    private final AsyncFunction<Object, MoveChannelOperation> returnThis = new AsyncFunction<Object, MoveChannelOperation>() {
        @Override
        public ListenableFuture<MoveChannelOperation> apply(Object input)
                throws Exception {
            return immediateFutureForThis;
        }
    };
    private UUID thisServerId;
    private boolean verifiedOldServerOffline;

    private MoveChannelOperation(ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, MoveChannelCommand command) {
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
        this.expectedOldServerId = command.getExpectedOldServerId();
        this.channelName = command.getChannelName();
        this.newServerId = command.getNewServerId();
    }

    /**
     * Moves a channel from one server to another one. The operation is
     * performed in an asynchronous way. The result is indicated through the
     * returned future. In case of error, the returned future throws an
     * exception.
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
     *            move-channel command that shall be executed as part of this
     *            operation.
     * @param prefetchedChannelInformation
     *            reasonably up-to-date channel information for the specified
     *            channel. This information is used to determine whether the
     *            channel exists and which server currently owns it. If the
     *            channel does not exist (the channel information is
     *            <code>null</code>) the operation fails immediately.
     * @return future through which the completion of the operation can be
     *         monitored. If the operation resulted in the channel being
     *         modified, the future returns <code>true</code>. If the channel
     *         was not modified, the future returns <code>false</code>. Please
     *         note that the return value only indicates whether the operation
     *         modified the channel directly. If it was forwarded to a remote
     *         server and that server modified the channel, the future will
     *         still return <code>false</code>.
     */
    public static ListenableFuture<Boolean> moveChannel(
            ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, MoveChannelCommand command,
            ChannelInformation prefetchedChannelInformation) {
        MoveChannelOperation operation = new MoveChannelOperation(
                archivingService, channelInformationCache, channelMetaDataDAO,
                clusterManagementService, interNodeCommunicationService,
                thisServerId, command);
        operation.channelInformation = prefetchedChannelInformation;
        operation.needChannelInformation = false;
        try {
            return Futures
                    .transform(MOVE_CHANNEL.apply(operation), IS_MODIFIED);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
