/*
 * Copyright 2015-2017 aquenos GmbH.
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
import java.util.LinkedList;
import java.util.List;
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
import com.aquenos.cassandra.pvarchiver.server.archiving.RemoveChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.SampleBucketInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Asynchronous operation for removing a channel. The operation is encapsulated
 * in a class because it consists of many steps that are chained in an
 * asynchronous fashion in order to avoid blocking a thread for an extended
 * amount of time. This class is only intended for use by the
 * {@link ArchiveConfigurationService}.
 * 
 * @author Sebastian Marsching
 */
public final class RemoveChannelOperation {

    private static final Function<Object, Boolean> ALWAYS_TRUE = Functions
            .constant(true);

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> CREATE_GLOBAL_PENDING_OPERATION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperationRelaxed(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.channelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_DELETE,
                            "",
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, RemoveChannelOperation>() {
                                @Override
                                public ListenableFuture<RemoveChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdGlobalPendingOperation = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> CREATE_LOCAL_PENDING_OPERATION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperationRelaxed(
                            operation.serverId,
                            operation.channelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_DELETE,
                            "",
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, RemoveChannelOperation>() {
                                @Override
                                public ListenableFuture<RemoveChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdLocalPendingOperation = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final Predicate<RemoveChannelOperation> CREATED_GLOBAL_PENDING_OPERATION = new Predicate<RemoveChannelOperation>() {
        @Override
        public boolean apply(RemoveChannelOperation operation) {
            return operation.createdGlobalPendingOperation;
        }
    };

    private static final Predicate<RemoveChannelOperation> CREATED_LOCAL_PENDING_OPERATION = new Predicate<RemoveChannelOperation>() {
        @Override
        public boolean apply(RemoveChannelOperation operation) {
            return operation.createdLocalPendingOperation;
        }
    };

    private static final Predicate<RemoveChannelOperation> CONTROL_SYSTEM_SUPPORT_MISSING = new Predicate<RemoveChannelOperation>() {
        @Override
        public boolean apply(RemoveChannelOperation operation) {
            return operation.controlSystemSupport == null;
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> DELETE_GLOBAL_PENDING_OPERATION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .deletePendingChannelOperation(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.channelName, operation.operationId);
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> DELETE_LOCAL_PENDING_OPERATION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .deletePendingChannelOperation(operation.serverId,
                            operation.channelName, operation.operationId);
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> DELETE_PENDING_OPERATIONS = compose(
            aggregate(DELETE_GLOBAL_PENDING_OPERATION,
                    DELETE_LOCAL_PENDING_OPERATION),
            new Function<List<RemoveChannelOperation>, RemoveChannelOperation>() {
                @Override
                public RemoveChannelOperation apply(
                        List<RemoveChannelOperation> input) {
                    return input.get(0);
                }
            });

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> DELETE_SAMPLE_BUCKETS = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            // We have to delete the actual sample buckets before deleting the
            // meta-data. Otherwise, there would be no way to delete them later
            // because we would not be able to identify them.
            // After receiving the list of buckets, we run the function using
            // the passed executor. If a channel has a lot of samples, the list
            // of sample buckets might be long and we might spend a considerable
            // amount of time starting the delete operations.
            return Futures
                    .transform(
                            operation.channelMetaDataDAO
                                    .getSampleBuckets(operation.channelName),
                            new AsyncFunction<Iterable<? extends SampleBucketInformation>, RemoveChannelOperation>() {
                                @Override
                                public ListenableFuture<RemoveChannelOperation> apply(
                                        Iterable<? extends SampleBucketInformation> input)
                                        throws Exception {
                                    LinkedList<ListenableFuture<Void>> deleteSampleBucketFutures = new LinkedList<ListenableFuture<Void>>();
                                    for (SampleBucketInformation bucketInformation : input) {
                                        // If deleteSamples throws an exception
                                        // (it should not), this method throws
                                        // an exception and the whole operation
                                        // will fail. This is okay because such
                                        // a behavior violates the contract of
                                        // the ControlSystemSupport interface.
                                        ListenableFuture<Void> future = operation.controlSystemSupport
                                                .deleteSamples(bucketInformation
                                                        .getBucketId());
                                        // deleteSamples should not return null
                                        // either.
                                        if (future == null) {
                                            throw new NullPointerException(
                                                    "The control-system support's deleteSamples method returned null.");
                                        }
                                        deleteSampleBucketFutures.add(future);
                                    }
                                    return Futures.transform(
                                            Futures.allAsList(deleteSampleBucketFutures),
                                            operation.returnThis);
                                }
                            }, operation.executor);
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> FORWARD_TO_SERVER = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            return Futures
                    .transform(
                            operation.interNodeCommunicationService
                                    .runArchiveConfigurationCommands(
                                            operation.serverBaseUrl,
                                            Collections
                                                    .singletonList(operation.command)),
                            new Function<List<ArchiveConfigurationCommandResult>, RemoveChannelOperation>() {
                                @Override
                                public RemoveChannelOperation apply(
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
                                                        + " reported an error when trying to remove channel \""
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
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> GET_CHANNEL_INFORMATION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            return Futures.transform(operation.channelInformationCache
                    .getChannelAsync(operation.channelName, true),
                    new Function<ChannelInformation, RemoveChannelOperation>() {
                        @Override
                        public RemoveChannelOperation apply(
                                ChannelInformation input) {
                            operation.channelInformation = input;
                            operation.needChannelInformation = false;
                            if (input != null) {
                                operation.controlSystemSupport = operation.controlSystemSupportRegistry
                                        .getControlSystemSupport(input
                                                .getControlSystemType());
                            }
                            return operation;
                        }
                    });
        }
    };

    private static final Predicate<RemoveChannelOperation> NEED_CHANNEL_INFORMATION = new Predicate<RemoveChannelOperation>() {
        @Override
        public boolean apply(RemoveChannelOperation operation) {
            return operation.needChannelInformation;
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> PREPARE_PENDING_OPERATION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            operation.operationId = UUID.randomUUID();
            operation.pendingOperationCreatedTime = System.currentTimeMillis();
            return operation.immediateFutureForThis;
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REFRESH_CHANNEL = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            if (operation.thisServerId.equals(operation.serverId)) {
                return Futures.transform(operation.archivingService
                        .refreshChannel(operation.channelName),
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
                                                                    operation.channelName,
                                                                    operation.serverId))),
                                    operation.returnThis);
                } else {
                    return operation.immediateFutureForThis;
                }
            }
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP1 = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            // We can only remove a channel that already exists, so if we did
            // not get the channel information, we throw an exception.
            if (operation.channelInformation == null) {
                return THROW_NO_SUCH_CHANNEL_EXCEPTION.apply(operation);
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
            operation.serverBaseUrl = operation.clusterManagementService
                    .getInterNodeCommunicationUrl(operation.serverId);
            if (!operation.thisServerId.equals(operation.serverId)
                    && operation.serverBaseUrl != null) {
                return FORWARD_TO_SERVER.apply(operation);
            } else {
                return REMOVE_CHANNEL_STEP2.apply(operation);
            }
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP3 = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {

        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            // The channel configuration might have changed since we got it the
            // last time (before creating the pending operation). Therefore, we
            // basically have to run all checks again.
            // We can only move a channel that already exists, so if we did not
            // get the channel information, we throw an exception. However, we
            // first remove the pending operation.
            if (operation.channelInformation == null) {
                return REMOVE_CHANNEL_STEP4A.apply(operation);
            }
            // If the server that owns the channel changed, we registered the
            // pending operation for the old server. We have to delete the
            // pending operation and start again from the beginning.
            if (!operation.serverId.equals(operation.channelInformation
                    .getServerId())) {
                return REMOVE_CHANNEL_STEP4B.apply(operation);
            }
            // If the channel is not registered with this server, we have to
            // make sure that the owning server is still offline.
            if (!operation.thisServerId.equals(operation.serverId)) {
                return REMOVE_CHANNEL_STEP4C.apply(operation);
            } else {
                return REMOVE_CHANNEL_STEP4D.apply(operation);
            }
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> RENAME_CHANNEL_STEP6 = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            return Futures.transform(Futures.transform(
                    operation.channelMetaDataDAO.deleteChannel(
                            operation.channelName, operation.serverId),
                    operation.returnThis), REMOVE_CHANNEL_STEP7);
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> THROW_CONTROL_SYSTEM_SUPPORT_MISSING = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            throw new IllegalStateException("The control-sytem support \""
                    + StringEscapeUtils.escapeJava(operation.channelInformation
                            .getControlSystemType()) + "\" is not available.");
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> THROW_IF_TIMEOUT = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            // If more than two minutes have passed since creating the pending
            // operation, something is going wrong. If we continued, we would
            // risk making changes after the pending operation has expired, so
            // we rather throw an exception.
            if (System.currentTimeMillis() > operation.pendingOperationCreatedTime + 120000L) {
                throw new RuntimeException(
                        "Channel \""
                                + StringEscapeUtils
                                        .escapeJava(operation.channelName)
                                + "\" cannot be removed because the operation took too long to finish.");
            } else {
                return operation.immediateFutureForThis;
            }
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> THROW_NO_SUCH_CHANNEL_EXCEPTION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            throw new NoSuchChannelException("Channel \""
                    + StringEscapeUtils.escapeJava(operation.channelName)
                    + "\" cannot be removed because it does not exist.");
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> THROW_PENDING_OPERATION_EXCEPTION = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            throw new PendingChannelOperationException(
                    "Channel \""
                            + StringEscapeUtils
                                    .escapeJava(operation.channelName)
                            + "\" cannot be removed because another operation for this channel is pending.");
        }
    };

    private static final Predicate<RemoveChannelOperation> VERIFIED_SERVER_OFFLINE = new Predicate<RemoveChannelOperation>() {
        @Override
        public boolean apply(RemoveChannelOperation operation) {
            return operation.verifiedServerOffline;
        }
    };

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> VERIFY_SERVER_OFFLINE = new AsyncFunction<RemoveChannelOperation, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(
                final RemoveChannelOperation operation) throws Exception {
            return Futures.transform(operation.clusterManagementService
                    .verifyServerOffline(operation.serverId),
                    new AsyncFunction<Boolean, RemoveChannelOperation>() {
                        @Override
                        public ListenableFuture<RemoveChannelOperation> apply(
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

    // This function starts the remove operation. If gets the channel
    // information (if needed) and delegates to REMOVE_CHANNEL_STEP1.
    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL = compose(
            ifThen(NEED_CHANNEL_INFORMATION, GET_CHANNEL_INFORMATION),
            REMOVE_CHANNEL_STEP1);

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP7 = compose(
            DELETE_PENDING_OPERATIONS, REFRESH_CHANNEL);

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP5 = compose(
            CREATE_GLOBAL_PENDING_OPERATION,
            ifThenElse(
                    CREATED_GLOBAL_PENDING_OPERATION,
                    compose(THROW_IF_TIMEOUT, DELETE_SAMPLE_BUCKETS,
                            THROW_IF_TIMEOUT, RENAME_CHANNEL_STEP6),
                    compose(DELETE_LOCAL_PENDING_OPERATION,
                            THROW_PENDING_OPERATION_EXCEPTION)));

    // This function is called if the channel has been deleted before we got the
    // initial channel information but before we registered the pending
    // operation.
    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP4A = compose(
            DELETE_LOCAL_PENDING_OPERATION, THROW_NO_SUCH_CHANNEL_EXCEPTION);

    // This function is called if the channel has been moved to a different
    // server before we registered the pending operation. This means we have to
    // start again by delegating to REMOVE_CHANNEL.
    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP4B = compose(
            DELETE_LOCAL_PENDING_OPERATION, REMOVE_CHANNEL);

    // This function is called in case the channel is registered with a
    // different server but this server is offline. In this case we have to
    // verify that the server is really offline and delegate to the other server
    // if it is now online.
    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP4C = compose(
            VERIFY_SERVER_OFFLINE,
            ifThenElse(
                    VERIFIED_SERVER_OFFLINE,
                    ifThenElse(
                            CONTROL_SYSTEM_SUPPORT_MISSING,
                            compose(DELETE_LOCAL_PENDING_OPERATION,
                                    THROW_CONTROL_SYSTEM_SUPPORT_MISSING),
                            REMOVE_CHANNEL_STEP5),
                    compose(DELETE_LOCAL_PENDING_OPERATION, FORWARD_TO_SERVER)));

    // This function is called if the channel is local (the regular case).
    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP4D = ifThenElse(
            CONTROL_SYSTEM_SUPPORT_MISSING,
            compose(DELETE_LOCAL_PENDING_OPERATION,
                    THROW_CONTROL_SYSTEM_SUPPORT_MISSING),
            compose(REFRESH_CHANNEL, REMOVE_CHANNEL_STEP5));

    private static final AsyncFunction<RemoveChannelOperation, RemoveChannelOperation> REMOVE_CHANNEL_STEP2 = ifThenElse(
            CONTROL_SYSTEM_SUPPORT_MISSING,
            THROW_CONTROL_SYSTEM_SUPPORT_MISSING,
            compose(PREPARE_PENDING_OPERATION,
                    CREATE_LOCAL_PENDING_OPERATION,
                    ifThenElse(
                            CREATED_LOCAL_PENDING_OPERATION,
                            compose(GET_CHANNEL_INFORMATION,
                                    REMOVE_CHANNEL_STEP3),
                            THROW_PENDING_OPERATION_EXCEPTION)));

    private ArchivingService archivingService;
    private ChannelInformation channelInformation;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private String channelName;
    private ClusterManagementService clusterManagementService;
    private RemoveChannelCommand command;
    private ControlSystemSupport<?> controlSystemSupport;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private boolean createdGlobalPendingOperation;
    private boolean createdLocalPendingOperation;
    private Executor executor;
    private UUID expectedServerId;
    private final ListenableFuture<RemoveChannelOperation> immediateFutureForThis = Futures
            .immediateFuture(this);
    private InterNodeCommunicationService interNodeCommunicationService;
    private boolean needChannelInformation = true;
    private UUID operationId;
    private long pendingOperationCreatedTime;
    private final AsyncFunction<Object, RemoveChannelOperation> returnThis = new AsyncFunction<Object, RemoveChannelOperation>() {
        @Override
        public ListenableFuture<RemoveChannelOperation> apply(Object input)
                throws Exception {
            return immediateFutureForThis;
        }
    };
    private String serverBaseUrl;
    private UUID serverId;
    private UUID thisServerId;
    private boolean verifiedServerOffline;

    private RemoveChannelOperation(ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            ControlSystemSupportRegistry controlSystemSupportRegistry,
            Executor executor,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, RemoveChannelCommand command) {
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
        this.channelName = command.getChannelName();
        this.expectedServerId = command.getExpectedServerId();
    }

    /**
     * Removes a channel from the archive configuration. The operation is
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
     * @param controlSystemSupportRegistry
     *            control-system support registry for this server. The operation
     *            needs access to control-system supports because it has to
     *            delete all sample buckets for the channel before it can delete
     *            the actual channel.
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
     *            remove-channel command that shall be executed as part of this
     *            operation.
     * @param prefetchedChannelInformation
     *            reasonably up-to-date channel information for the specified
     *            channel. This information is used to determine whether the
     *            channel exists. If the channel does not exist (the channel
     *            information is <code>null</code>) the operation fails
     *            immediately.
     * @return future through which the completion of the operation can be
     *         monitored.
     */
    public static ListenableFuture<Boolean> removeChannel(
            ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            ControlSystemSupportRegistry controlSystemSupportRegistry,
            Executor executor,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, RemoveChannelCommand command,
            ChannelInformation prefetchedChannelInformation) {
        RemoveChannelOperation operation = new RemoveChannelOperation(
                archivingService, channelInformationCache, channelMetaDataDAO,
                clusterManagementService, controlSystemSupportRegistry,
                executor, interNodeCommunicationService, thisServerId, command);
        operation.channelInformation = prefetchedChannelInformation;
        if (operation.channelInformation != null) {
            operation.controlSystemSupport = controlSystemSupportRegistry
                    .getControlSystemSupport(operation.channelInformation
                            .getControlSystemType());
        }
        operation.needChannelInformation = false;
        try {
            // This operation always modifies the channel (or throws an
            // exception), so we can always return true. We only return Boolean
            // instead of Void so that this method's signature is compatible
            // with all other operations.
            return Futures.transform(REMOVE_CHANNEL.apply(operation),
                    ALWAYS_TRUE);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
