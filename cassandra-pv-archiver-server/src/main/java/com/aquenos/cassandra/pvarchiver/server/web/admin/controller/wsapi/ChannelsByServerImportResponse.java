/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 * Response object for the "import channel configuration" function of the
 * web-service API.
 * </p>
 * 
 * <p>
 * This object is primarily intended to facilitate the JSON serialization of the
 * data provided by the API controller. For this reason, it does not perform any
 * checks on the parameters used to construct the object.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelsByServerImportResponse {

    private final Map<String, String> addOrUpdateFailed;
    private final Set<String> addOrUpdateSucceeded;
    private final String errorMessage;
    private final Map<String, String> removeFailed;
    private final Set<String> removeSucceeded;

    /**
     * Creates a new response. This constructor does not verify the validity of
     * the arguments and simply uses them as-is.
     * 
     * @param addOrUpdateFailed
     *            channels for which an add or update operation failed. The keys
     *            are the channel names and the values are the respective error
     *            messages. <code>null</code> if running in simulation mode or
     *            if there is a global error.
     * @param addOrUpdateSucceeded
     *            channels for which an add or update operation succeeded.
     *            <code>null</code> if there is a global error.
     * @param errorMessage
     *            error message indicating a global error (an error that
     *            happened before changes where made to the configuration).
     *            <code>null</code> if there is no global error (there might
     *            still be errors for individual channels).
     * @param removeFailed
     *            channels for which a remove operation failed. The keys are the
     *            channel names and the values are the respective error
     *            messages. <code>null</code> if running in simulation mode or
     *            if there is a global error.
     * @param removeSucceeded
     *            channels for which a remove operation succeeded.
     *            <code>null</code> if there is a global error.
     */
    public ChannelsByServerImportResponse(
            @JsonProperty("addOrUpdateFailed") Map<String, String> addOrUpdateFailed,
            @JsonProperty("addOrUpdateSucceeded") Set<String> addOrUpdateSucceeded,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("removeFailed") Map<String, String> removeFailed,
            @JsonProperty("removeSucceeded") Set<String> removeSucceeded) {
        this.addOrUpdateFailed = addOrUpdateFailed;
        this.addOrUpdateSucceeded = addOrUpdateSucceeded;
        this.errorMessage = errorMessage;
        this.removeFailed = removeFailed;
        this.removeSucceeded = removeSucceeded;
    }

    /**
     * Returns the channels for which an add or update operation failed. The
     * keys are the channel names and the values are the respective error
     * messages. <code>null</code> if running in simulation mode or if there is
     * a global error.
     * 
     * @return map containing channel names with a failed add or update
     *         operation and the respective error messages.
     */
    public Map<String, String> getAddOrUpdateFailed() {
        return addOrUpdateFailed;
    }

    /**
     * Returns the channels for which an add or update operation succeeded.
     * <code>null</code> if there is a global error.
     * 
     * @return channels for which an add or update operation succeeded.
     */
    public Set<String> getAddOrUpdateSucceeded() {
        return addOrUpdateSucceeded;
    }

    /**
     * Returns the error message indicating a global error (an error that
     * happened before changed where made to the configuration).
     * <code>null</code> if there is no global error (there might still be
     * errors for individual channels).
     * 
     * @return global error message.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns the channels for which a remove operation failed. The keys are
     * the channel names and the values are the respective error messages.
     * <code>null</code> if running in simulation mode or if there is a global
     * error.
     * 
     * @return map containing channel names with a failed remove operation and
     *         the respective error messages.
     */
    public Map<String, String> getRemoveFailed() {
        return removeFailed;
    }

    /**
     * Returns the channels for which a remove operation succeeded.
     * <code>null</code> if there is a global error.
     * 
     * @return channels for which a remove operation succeeded.
     */
    public Set<String> getRemoveSucceeded() {
        return removeSucceeded;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(addOrUpdateFailed)
                .append(addOrUpdateSucceeded).append(errorMessage)
                .append(removeFailed).append(removeSucceeded).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ChannelsByServerImportResponse other = (ChannelsByServerImportResponse) obj;
        return new EqualsBuilder()
                .append(this.addOrUpdateFailed, other.addOrUpdateFailed)
                .append(this.addOrUpdateSucceeded, other.addOrUpdateSucceeded)
                .append(this.errorMessage, other.errorMessage)
                .append(this.removeFailed, other.removeFailed)
                .append(this.removeSucceeded, other.removeSucceeded).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
