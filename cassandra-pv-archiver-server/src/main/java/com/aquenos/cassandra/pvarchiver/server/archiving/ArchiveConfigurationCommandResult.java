/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Result of an {@link ArchiveConfigurationCommand}. When an archive
 * configuration command is executed, this result object is returned in order to
 * provide an indication of whether the operation was successful or failed. In
 * case of failure, an optional error message may be supplied.
 * </p>
 * 
 * <p>
 * Instances of this class must be created through the
 * {@link #success(ArchiveConfigurationCommand)} and
 * {@link #failure(ArchiveConfigurationCommand, String)} methods.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public final class ArchiveConfigurationCommandResult {

    private ArchiveConfigurationCommand command;
    private String errorMessage;
    private boolean success;

    private ArchiveConfigurationCommandResult(
            ArchiveConfigurationCommand command, String errorMessage,
            boolean success) {
        Preconditions.checkNotNull(command);
        this.command = command;
        this.errorMessage = errorMessage;
        this.success = success;
    }

    /**
     * Creates a result representing the specified arguments. This constructor
     * is mainly provided for automated construction from a serialized
     * representation (e.g. JSON). Code wanting to construct an instance of this
     * class should typically prefer the
     * {@link #success(ArchiveConfigurationCommand)} and
     * {@link #failure(ArchiveConfigurationCommand, String)} methods.
     * 
     * @param command
     *            command that was executed.
     * @param success
     *            <code>true</code> if the execution was successful,
     *            <code>false</code> if it failed.
     * @param errorMessage
     *            optional error message describing the cause of the failure.
     *            Must be <code>null</code> if <code>success</code> is
     *            <code>true</code>.
     * @return configuration command result representing the specified
     *         arguments.
     * @throws IllegalArgumentException
     *             if <code>success</code> is <code>true</code> and
     *             <code>errorMessage</code> is not <code>null</code>.
     * @throws NullPointerException
     *             if <code>command</code> is <code>null</code>.
     * @see #failure(ArchiveConfigurationCommand, String)
     * @see #success(ArchiveConfigurationCommand)
     */
    @JsonCreator
    public static ArchiveConfigurationCommandResult create(
            @JsonProperty(value = "command", required = true) ArchiveConfigurationCommand command,
            @JsonProperty(value = "success", required = true) boolean success,
            @JsonProperty("errorMessage") String errorMessage) {
        Preconditions.checkArgument(success == false || errorMessage == null,
                "The errorMessage must be null if success is true.");
        return new ArchiveConfigurationCommandResult(command, errorMessage,
                success);
    }

    /**
     * Creates a result that represents a successful execution of the specified
     * command.
     * 
     * @param command
     *            command that was executed successfully.
     * @return configuration command result representing the successful
     *         execution of the specified command.
     * @throws NullPointerException
     *             if <code>command</code> is <code>null</code>.
     */
    public static ArchiveConfigurationCommandResult success(
            ArchiveConfigurationCommand command) {
        return new ArchiveConfigurationCommandResult(command, null, true);
    }

    /**
     * Creates a result that represents a failed execution of the specified
     * command. If not <code>null</code>, the specified error message is
     * attached to the result.
     * 
     * @param command
     *            command whose execution failed.
     * @param errorMessage
     *            optional error message describing the cause of the failure or
     *            <code>null</code> if no such message can be provided.
     * @return configuration command result representing the failed execution of
     *         the specified command.
     * @throws NullPointerException
     *             if <code>command</code> is <code>null</code>.
     */
    public static ArchiveConfigurationCommandResult failure(
            ArchiveConfigurationCommand command, String errorMessage) {
        return new ArchiveConfigurationCommandResult(command, errorMessage,
                false);
    }

    /**
     * Returns the command for which this object represents the result of the
     * execution. The returned value is never <code>null</code>.
     * 
     * @return command object that belongs to this result.
     */
    public ArchiveConfigurationCommand getCommand() {
        return command;
    }

    /**
     * Returns the optional error message associated with this result. If the
     * execution of the command was successful ({@link #isSuccess()} returns
     * <code>true</code>), the returned value is always <code>null</code>. If
     * the execution of the command failed, the return value might still be
     * <code>null</code> if no error message was provided.
     * 
     * @return error message associated with the failed execution of the command
     *         or <code>null</code> if no such message is available.
     */
    @JsonInclude(Include.NON_NULL)
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Tells whether the execution of the command was successful.
     * 
     * @return <code>true</code> if the command was executed successfully and
     *         <code>false</code> if the execution failed.
     */
    public boolean isSuccess() {
        return success;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(command).append(errorMessage)
                .append(success).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ArchiveConfigurationCommandResult other = (ArchiveConfigurationCommandResult) obj;
        return new EqualsBuilder().append(this.command, other.command)
                .append(this.errorMessage, other.errorMessage)
                .append(this.success, other.success).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
