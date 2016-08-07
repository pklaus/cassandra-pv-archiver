/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/**
 * <p>
 * Base class for all commands that update the archive configuration. This base
 * class and its children are designed to build a closed class-hierarchy for all
 * commands related to archive configuration operations.
 * </p>
 * 
 * <p>
 * The {@link #getCommandType()} method can be used to identify the exact type
 * of the command and to cast it to that type subsequently. This method returns
 * one of the following enum values, representing the respective class.
 * </p>
 * 
 * <table summary="configuration-command implementations and their corresponding type identifiers">
 * <tr>
 * <th>Command Type String</th>
 * <th>Command Class</th>
 * </tr>
 * <tr>
 * <td>{@link Type#ADD_CHANNEL}</td>
 * <td>{@link AddChannelCommand}</td>
 * </tr>
 * <tr>
 * <td>{@link Type#ADD_OR_UPDATE_CHANNEL}</td>
 * <td>{@link AddOrUpdateChannelCommand}</td>
 * </tr>
 * <tr>
 * <td>{@link Type#MOVE_CHANNEL}</td>
 * <td>{@link MoveChannelCommand}</td>
 * </tr>
 * <tr>
 * <td>{@link Type#REFRESH_CHANNEL}</td>
 * <td>{@link RefreshChannelCommand}</td>
 * </tr>
 * <tr>
 * <td>{@link Type#REMOVE_CHANNEL}</td>
 * <td>{@link RemoveChannelCommand}</td>
 * </tr>
 * <tr>
 * <td>{@link Type#RENAME_CHANNEL}</td>
 * <td>{@link RenameChannelCommand}</td>
 * </tr>
 * <tr>
 * <td>{@link Type#UPDATE_CHANNEL}</td>
 * <td>{@link UpdateChannelCommand}</td>
 * </tr>
 * </table>
 * 
 * @author Sebastian Marsching
 */
@JsonSubTypes({
        @JsonSubTypes.Type(value = AddChannelCommand.class, name = "add_channel"),
        @JsonSubTypes.Type(value = AddOrUpdateChannelCommand.class, name = "add_or_update_channel"),
        @JsonSubTypes.Type(value = MoveChannelCommand.class, name = "move_channel"),
        @JsonSubTypes.Type(value = RefreshChannelCommand.class, name = "refresh_channel"),
        @JsonSubTypes.Type(value = RemoveChannelCommand.class, name = "remove_channel"),
        @JsonSubTypes.Type(value = RenameChannelCommand.class, name = "rename_channel"),
        @JsonSubTypes.Type(value = UpdateChannelCommand.class, name = "update_channel") })
@JsonTypeInfo(use = Id.NAME, property = "commandType")
public abstract class ArchiveConfigurationCommand {

    /**
     * Type of an {@link ArchiveConfigurationCommand}. As the
     * {@link ArchiveConfigurationCommand} and its child classes build a closed
     * hierarchy, every class in the hierarchy maps to exactly one of the types
     * and vice-versa.
     * 
     * @author Sebastian Marsching
     */
    public static enum Type {

        /**
         * Type of the {@link AddChannelCommand}.
         */
        ADD_CHANNEL,

        /**
         * Type of the {@link AddOrUpdateChannelCommand}.
         */
        ADD_OR_UPDATE_CHANNEL,

        /**
         * Type of the {@link MoveChannelCommand}.
         */
        MOVE_CHANNEL,

        /**
         * Type of the {@link RefreshChannelCommand}.
         */
        REFRESH_CHANNEL,

        /**
         * Type of the {@link RemoveChannelCommand}.
         */
        REMOVE_CHANNEL,

        /**
         * Type of the {@link RenameChannelCommand}.
         */
        RENAME_CHANNEL,

        /**
         * Type of the {@link UpdateChannelCommand}.
         */
        UPDATE_CHANNEL

    }

    /**
     * Default constructor. The constructor is package private so that only
     * classes in the same package can inherit from this class.
     */
    ArchiveConfigurationCommand() {
    }

    /**
     * Returns the type of the actual command type. Please refer to the document
     * in the {@link ArchiveConfigurationCommand} class or the {@link Type} enum
     * for a list of types. The return value is never <code>null</code> .
     * 
     * @return type of this command.
     */
    @JsonIgnore
    public abstract Type getCommandType();

}
