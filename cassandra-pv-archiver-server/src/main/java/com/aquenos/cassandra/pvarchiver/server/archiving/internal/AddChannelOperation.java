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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.server.archiving.AddChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelAlreadyExistsException;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.PendingChannelOperationException;
import com.aquenos.cassandra.pvarchiver.server.archiving.RefreshChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Asynchronous operation for adding a channel. The operation is encapsulated in
 * a class because it consists of many steps that are chained in an asynchronous
 * fashion in order to avoid blocking a thread for an extended amount of time.
 * This class is only intended for use by the
 * {@link ArchiveConfigurationService}.
 * 
 * @author Sebastian Marsching
 */
public final class AddChannelOperation {

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> ADD_CHANNEL_STEP1 = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            // The calling code already ensures that decimation level zero is
            // always created. Therefore, we do not have to check this here.
            operation.modified = true;
            return Futures.transform(Futures.transform(
                    operation.channelMetaDataDAO.createChannel(
                            operation.channelName, operation.channelDataId,
                            operation.controlSystemType,
                            operation.decimationLevels,
                            operation.decimationLevelToRetentionPeriod,
                            operation.enabled, operation.options,
                            operation.serverId), operation.returnThis),
                    ADD_CHANNEL_STEP2);
        }
    };

    private static final Function<Object, Boolean> ALWAYS_TRUE = Functions
            .constant(true);

    private static final Predicate<AddChannelOperation> CHANNEL_INFORMATION_NOT_NULL = new Predicate<AddChannelOperation>() {
        @Override
        public boolean apply(AddChannelOperation operation) {
            return operation.channelInformation != null;
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> CREATE_GLOBAL_PENDING_OPERATION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            operation.pendingOperationCreatedTime = System.currentTimeMillis();
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            PendingChannelOperationConstants.UNASSIGNED_SERVER_ID,
                            operation.channelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_CREATE,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, AddChannelOperation>() {
                                @Override
                                public ListenableFuture<AddChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdGlobalPendingOperation = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> CREATE_LOCAL_PENDING_OPERATION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            ListenableFuture<Pair<Boolean, UUID>> future = operation.channelMetaDataDAO
                    .createPendingChannelOperation(
                            operation.serverId,
                            operation.channelName,
                            operation.operationId,
                            PendingChannelOperationConstants.OPERATION_CREATE,
                            operation.operationData,
                            PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
            return Futures
                    .transform(
                            future,
                            new AsyncFunction<Pair<Boolean, UUID>, AddChannelOperation>() {
                                @Override
                                public ListenableFuture<AddChannelOperation> apply(
                                        Pair<Boolean, UUID> input)
                                        throws Exception {
                                    operation.createdLocalPendingOperation = input
                                            .getLeft();
                                    return operation.immediateFutureForThis;
                                }
                            });
        }
    };

    private static final Predicate<AddChannelOperation> CREATED_GLOBAL_PENDING_OPERATION = new Predicate<AddChannelOperation>() {
        @Override
        public boolean apply(AddChannelOperation operation) {
            return operation.createdGlobalPendingOperation;
        }
    };

    private static final Predicate<AddChannelOperation> CREATED_LOCAL_PENDING_OPERATION = new Predicate<AddChannelOperation>() {
        @Override
        public boolean apply(AddChannelOperation operation) {
            return operation.createdLocalPendingOperation;
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> DELETE_GLOBAL_PENDING_OPERATION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            // We can always delete the global pending operation. Another server
            // will see the channel that has been created and thus check for a
            // local pending operation.
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

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> DELETE_LOCAL_PENDING_OPERATION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
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
                        .deletePendingChannelOperation(operation.serverId,
                                operation.channelName, operation.operationId);
            }
            // We do not care about the return value of the delete operation:
            // Even if the pending operation could not be deleted (because it
            // did not exist any longer), we regard this as success.
            return Futures.transform(future, operation.returnThis);
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> DELETE_PENDING_OPERATIONS = compose(
            aggregate(DELETE_GLOBAL_PENDING_OPERATION,
                    DELETE_LOCAL_PENDING_OPERATION),
            new Function<List<AddChannelOperation>, AddChannelOperation>() {
                @Override
                public AddChannelOperation apply(List<AddChannelOperation> input) {
                    return input.get(0);
                }
            });

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> GET_CHANNEL_INFORMATION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            return Futures.transform(operation.channelInformationCache
                    .getChannelAsync(operation.channelName, true),
                    new Function<ChannelInformation, AddChannelOperation>() {
                        @Override
                        public AddChannelOperation apply(
                                ChannelInformation input) {
                            operation.channelInformation = input;
                            operation.needChannelInformation = false;
                            return operation;
                        }
                    });
        }
    };

    private static final Predicate<AddChannelOperation> NEED_CHANNEL_INFORMATION = new Predicate<AddChannelOperation>() {
        @Override
        public boolean apply(AddChannelOperation operation) {
            return operation.needChannelInformation;
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> PREPARE_PENDING_OPERATION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            operation.operationId = UUID.randomUUID();
            StringBuilder operationDataBuilder = new StringBuilder();
            operationDataBuilder.append("{ \"serverId\": \"");
            operationDataBuilder.append(operation.serverId.toString());
            operationDataBuilder.append("\" }");
            operation.operationData = operationDataBuilder.toString();
            return operation.immediateFutureForThis;
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> REFRESH_CHANNEL = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
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

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> THROW_CHANNEL_ALREADY_EXISTS_EXCEPTION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            throw new ChannelAlreadyExistsException(
                    "Channel \""
                            + StringEscapeUtils
                                    .escapeJava(operation.channelName)
                            + "\" cannot be added because a channel with the same name already exists.");
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> THROW_IF_TIMEOUT = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            // If more than two minutes have passed since creating the pending
            // operation, something is going wrong. If we continued, we would
            // risk making changes after the pending operation has expired, so
            // we rather throw an exception.
            if (System.currentTimeMillis() > operation.pendingOperationCreatedTime + 120000L) {
                throw new RuntimeException(
                        "Channel \""
                                + StringEscapeUtils
                                        .escapeJava(operation.channelName)
                                + "\" cannot be added because the operation took too long to finish.");
            } else {
                return operation.immediateFutureForThis;
            }
        }
    };

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> THROW_PENDING_OPERATION_EXCEPTION = new AsyncFunction<AddChannelOperation, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(
                final AddChannelOperation operation) throws Exception {
            throw new PendingChannelOperationException(
                    "Channel \""
                            + StringEscapeUtils
                                    .escapeJava(operation.channelName)
                            + "\" cannot be added because another operation for this channel is pending.");
        }
    };

    // This asynchronous function takes care of all steps until the operation is
    // either cancelled with an error, forwarded to a different server, or
    // delegated ADD_CHANNEL_STEP1. In particular, it takes care of retrieving
    // the ChannelInformation and registering a pending operation. Please note
    // that when adding a channel, a global (unassigned server ID) and a local
    // (server for the channel) pending operation has to be created. The first
    // one ensures that no other server tries to add the same channel
    // concurrently. The second one ensures that the server will not use
    // the new channel until the whole operation has finished.
    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> ADD_CHANNEL = compose(
            ifThen(NEED_CHANNEL_INFORMATION, GET_CHANNEL_INFORMATION),
            ifThenElse(
                    CHANNEL_INFORMATION_NOT_NULL,
                    THROW_CHANNEL_ALREADY_EXISTS_EXCEPTION,
                    compose(PREPARE_PENDING_OPERATION,
                            CREATE_GLOBAL_PENDING_OPERATION,
                            ifThenElse(
                                    CREATED_GLOBAL_PENDING_OPERATION,
                                    compose(GET_CHANNEL_INFORMATION,
                                            ifThenElse(
                                                    CHANNEL_INFORMATION_NOT_NULL,
                                                    compose(DELETE_GLOBAL_PENDING_OPERATION,
                                                            THROW_CHANNEL_ALREADY_EXISTS_EXCEPTION),
                                                    compose(CREATE_LOCAL_PENDING_OPERATION,
                                                            ifThenElse(
                                                                    CREATED_LOCAL_PENDING_OPERATION,
                                                                    compose(THROW_IF_TIMEOUT,
                                                                            ADD_CHANNEL_STEP1),
                                                                    compose(DELETE_GLOBAL_PENDING_OPERATION,
                                                                            THROW_PENDING_OPERATION_EXCEPTION))))),
                                    THROW_PENDING_OPERATION_EXCEPTION))));

    private static final AsyncFunction<AddChannelOperation, AddChannelOperation> ADD_CHANNEL_STEP2 = compose(
            DELETE_PENDING_OPERATIONS, REFRESH_CHANNEL);

    private ArchivingService archivingService;
    private UUID channelDataId;
    private ChannelInformation channelInformation;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private String channelName;
    private ClusterManagementService clusterManagementService;
    private String controlSystemType;
    private boolean createdGlobalPendingOperation;
    private boolean createdLocalPendingOperation;
    private Set<Integer> decimationLevels;
    private Map<Integer, Integer> decimationLevelToRetentionPeriod;
    private boolean enabled;
    private final ListenableFuture<AddChannelOperation> immediateFutureForThis = Futures
            .immediateFuture(this);
    private InterNodeCommunicationService interNodeCommunicationService;
    private boolean modified;
    private boolean needChannelInformation = true;
    private String operationData;
    private UUID operationId;
    private Map<String, String> options;
    private long pendingOperationCreatedTime;
    private final AsyncFunction<Object, AddChannelOperation> returnThis = new AsyncFunction<Object, AddChannelOperation>() {
        @Override
        public ListenableFuture<AddChannelOperation> apply(Object input)
                throws Exception {
            return immediateFutureForThis;
        }
    };
    private UUID serverId;
    private UUID thisServerId;

    private AddChannelOperation(ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, AddChannelCommand command) {
        this.archivingService = archivingService;
        this.channelInformationCache = channelInformationCache;
        this.channelMetaDataDAO = channelMetaDataDAO;
        this.clusterManagementService = clusterManagementService;
        this.interNodeCommunicationService = interNodeCommunicationService;
        this.thisServerId = thisServerId;
        // The command object already performs validation of its parameters
        // during construction. Therefore, we can use all the values without any
        // additional checks.
        this.channelDataId = UUID.randomUUID();
        this.channelName = command.getChannelName();
        this.controlSystemType = command.getControlSystemType();
        this.decimationLevels = command.getDecimationLevels();
        this.decimationLevelToRetentionPeriod = command
                .getDecimationLevelToRetentionPeriod();
        this.enabled = command.isEnabled();
        this.options = command.getOptions();
        this.serverId = command.getServerId();
    }

    /**
     * Adds a channel to the archive configuration. The operation is performed
     * in an asynchronous way. The result is indicated through the returned
     * future. In case of error, the returned future throws an exception.
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
     *            add-channel command that shall be executed as part of this
     *            operation.
     * @param prefetchedChannelInformation
     *            reasonably up-to-date channel information for the specified
     *            channel. This information is used to determine whether the
     *            channel already exists. If the channel already exists (the
     *            channel information is not <code>null</code>) the operation
     *            fails immediately.
     * @return future through which the completion of the operation can be
     *         monitored.
     */
    public static ListenableFuture<Boolean> addChannel(
            ArchivingService archivingService,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService,
            UUID thisServerId, AddChannelCommand command,
            ChannelInformation prefetchedChannelInformation) {
        AddChannelOperation operation = new AddChannelOperation(
                archivingService, channelInformationCache, channelMetaDataDAO,
                clusterManagementService, interNodeCommunicationService,
                thisServerId, command);
        operation.channelInformation = prefetchedChannelInformation;
        operation.needChannelInformation = false;
        try {
            // This operation always modifies the channel (or throws an
            // exception), so we can always return true. We only return Boolean
            // instead of Void so that this method's signature is compatible
            // with all other operations.
            return Futures.transform(ADD_CHANNEL.apply(operation), ALWAYS_TRUE);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
