/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.UUID;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;

/**
 * Constants related to pending channel operations. This class is only intended
 * for internal use by classes in this package and by the
 * {@link ArchivingService}.
 * 
 * @author Sebastian Marsching
 */
public abstract class PendingChannelOperationConstants {

    /**
     * String identifying an add / create channel operation in the database.
     */
    public static final String OPERATION_CREATE = "create";

    /**
     * String identifying a create sample-bucket operation in the database.
     */
    public static final String OPERATION_CREATE_SAMPLE_BUCKET = "create_sample_bucket";

    /**
     * String identifying a remove / delete channel operation in the database.
     */
    public static final String OPERATION_DELETE = "delete";

    /**
     * String identifying a move channel operation in the database.
     */
    public static final String OPERATION_MOVE = "move";

    /**
     * String identifying a protective pending operation in the database. Unlike
     * the other modifications, this operation is not used to mark an operation
     * that is in progress but it marks that an operation has finished recently
     * and another operation should not be started too early. For this reason,
     * this pending operation should not be used with the regular TTL but with
     * the {@link #PROTECTIVE_PENDING_OPERATION_TTL_SECONDS}.
     */
    public static final String OPERATION_PROTECTIVE = "protective";

    /**
     * String identifying a rename channel operation in the database.
     */
    public static final String OPERATION_RENAME = "rename";

    /**
     * String identifying an update operation in the database.
     */
    public static final String OPERATION_UPDATE = "update";

    /**
     * <p>
     * Time-to-live for pending operation entries in the database (in seconds).
     * </p>
     * 
     * <p>
     * The TTL for pending operations has to be chosen so that we can be sure
     * that we see all batched updates in a consistent way. For successful
     * batched updates, this is simple: As soon as the batch succeeds, the
     * pending operation can (and will) be removed. However, in case of failure,
     * it is hard to tell whether the batch was or will be applied. It is
     * possible that the batch got written to the batch log and will be applied
     * with a delay. Cassandra will wait two times write timeout before retrying
     * a batch. However, it only checks once a minute whether there are any
     * batches to be run again.
     * </p>
     * 
     * <p>
     * Typically, the write timeout is in the order of a few seconds. This means
     * that we will typically see a consistent state about 70 to 80 seconds
     * after the last batch was started. Considering that there is some inherent
     * delay between registering a pending operation and running a batch, we
     * have to add some time to this. In addition to that, an operation might
     * actually consist of several batches which run serially, so we have to
     * wait at least a few minutes. The ten minutes we choose here are a
     * compromise between safety and getting a channel back online when there
     * was an unexpected failure at an inconvenient point in time.
     * </p>
     */
    public static final int PENDING_OPERATION_TTL_SECONDS = 600;

    /**
     * <p>
     * Time-to-live for protective pending operation entries in the database (in
     * seconds).
     * </p>
     * 
     * <p>
     * Unlike regular pending operations, the {@link #OPERATION_PROTECTIVE} does
     * not protect an actual operation in progress but is merely used to create
     * a protective buffer between two operations performed by different
     * servers. As Apache Cassandra uses a last-write-wins strategy based on
     * client-supplied time-stamps, a clock skew between two servers could lead
     * to a situation in which a later modification gets discarded because it
     * uses a time stamp that is before the time stamp of an earlier
     * modification.
     * </p>
     * 
     * <p>
     * We use a time of five seconds because this should be sufficient to
     * protect us from the typical clock skew that might exist in a cluster were
     * clocks are synchronized in general. We use the same period when
     * determining whether a server is online or offline. Unfortunately, we
     * cannot protect against an arbitrarily large clock skew because there is a
     * trade-off between the maximum clock skew we can protect against and the
     * rate at which we can make modifications.
     * </p>
     * 
     * <p>
     * We have a background service that tries to detect clock skew between
     * servers and shuts the server down if it detects a large clock skew. A
     * clock skew of more than five seconds should be large enough so that
     * typically this service will be able to detect it.
     * </p>
     */
    public static final int PROTECTIVE_PENDING_OPERATION_TTL_SECONDS = 5;

    /**
     * String representation of the {@link #UNASSIGNED_SERVER_ID}.
     */
    public static final String UNASSIGNED_SERVER_ID_STRING = "cccd4f6a-3311-4260-bdb6-0517eabe121d";

    /**
     * <p>
     * Unassigned server ID. This ID is used for registering pending operations
     * that are not restricted to a single server: When adding or removing a
     * channel, it has to be ensured that no other server interferes with the
     * operation. As another server might not see that the channel exists, it
     * cannot check the pending operations for a specific server. Therefore,
     * there needs to be a single point that all servers will check before
     * touching a channel that they think does not exist yet.
     * </p>
     * 
     * <p>
     * For a string representation of this ID, see
     * {@link #UNASSIGNED_SERVER_ID_STRING}.
     * </p>
     */
    public static final UUID UNASSIGNED_SERVER_ID = UUID
            .fromString(UNASSIGNED_SERVER_ID_STRING);

}
