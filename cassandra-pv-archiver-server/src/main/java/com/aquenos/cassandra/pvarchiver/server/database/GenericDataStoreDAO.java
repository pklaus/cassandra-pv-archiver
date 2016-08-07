/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Data access object that can be used by other component to store arbitrary
 * (small amounts of) data.
 * </p>
 * 
 * <p>
 * Once initialized, this DAO publishes a
 * {@link GenericDataStoreDAOInitializedEvent} in the application context, which
 * can be used by other beans that need to use this DAO.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface GenericDataStoreDAO {

    /**
     * Value object representing a single data item.
     * 
     * @author Sebastian Marsching
     */
    public class DataItem {

        private UUID componentId;
        private String key;
        private String value;

        /**
         * Creates a data item object.
         * 
         * @param componentId
         *            component ID. The component ID identifies the component
         *            with which the data item is associated. Its main purpose
         *            is to avoid naming collisions between different
         *            components.
         * @param key
         *            key of for this data item. The key is used to distinguish
         *            different data items belonging to the same component.
         * @param value
         *            value of this data item. The meaning of the value and its
         *            format is entirely up to the component using it.
         */
        public DataItem(UUID componentId, String key, String value) {
            this.componentId = componentId;
            this.key = key;
            this.value = value;
        }

        /**
         * Returns the component ID. The component ID identifies the component
         * with which the data item is associated. Its main purpose is to avoid
         * naming collisions between different components.
         * 
         * @return ID of the component to which this data item belongs.
         */
        public UUID getComponentId() {
            return componentId;
        }

        /**
         * Returns the key of for this data item. The key is used to distinguish
         * different data items belonging to the same component.
         * 
         * @return key identifying this data item within the component.
         */
        public String getKey() {
            return key;
        }

        /**
         * Returns the value of this data item. The meaning of the value and its
         * format is entirely up to the component using it.
         * 
         * @return value associated with this data item.
         */
        public String getValue() {
            return value;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(getComponentId())
                    .append(getKey()).append(getValue()).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DataItem)) {
                return false;
            }
            DataItem other = (DataItem) obj;
            return new EqualsBuilder()
                    .append(this.getComponentId(), other.getComponentId())
                    .append(this.getKey(), other.getKey())
                    .append(this.getValue(), other.getValue()).isEquals();
        }

    }

    /**
     * Create a data item for the specified component and key if no such item
     * exists yet. Typically, this is less efficient than
     * {@link #createOrUpdateItem(UUID, String, String)}, so code should prefer
     * the latter method unless it has to ensure that it does not overwrite an
     * existing value. The operation is performed in an asynchronous way so that
     * it will not block for network communication. The result of the operation
     * can be checked through the returned future.
     * 
     * @param componentId
     *            ID of the component for which the data item should be created.
     * @param key
     *            key identifying the data item within the component.
     * @param value
     *            value of the data item.
     * @return pair of a boolean flag and a string exposed through a future. The
     *         boolean flag indicates whether the item was created. If
     *         <code>true</code>, the item was created, if <code>false</code>,
     *         an item with the specified key already existed for the specified
     *         component. In this case, the string is the value of the existing
     *         item. In case of failure, the future's <code>get()</code> method
     *         will throw an exception.
     */
    ListenableFuture<Pair<Boolean, String>> createItem(UUID componentId,
            String key, String value);

    /**
     * Creates a data item or updates it if an item with the specified key
     * already exists for the specified component. This is the most efficient
     * way to create or update a data item and should be preferred over
     * {@link #createItem(UUID, String, String)} and
     * {@link #updateItem(UUID, String, String, String)}. The operation is
     * performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * 
     * @param componentId
     *            ID of the component for which the data item should be created
     *            or updated.
     * @param key
     *            key identifying the data item within the component.
     * @param value
     *            value of the data item.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> createOrUpdateItem(UUID componentId, String key,
            String value);

    /**
     * Removes all data items for the specified component. The operation is
     * performed in an asynchronous way so that it will not block for network
     * communication. The result of the operation can be checked through the
     * returned future.
     * 
     * @param componentId
     *            ID of the component for which all data items should be
     *            removed.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> deleteAllItems(UUID componentId);

    /**
     * Removes the item with the specified key from collection of data items for
     * the specified component. The operation is performed in an asynchronous
     * way so that it will not block for network communication. The result of
     * the operation can be checked through the returned future.
     * 
     * @param componentId
     *            ID of the component that owns the data item.
     * @param key
     *            key identifying the data item within the component.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> deleteItem(UUID componentId, String key);

    /**
     * <p>
     * Returns all data items for the specified component. The operation is
     * performed in an asynchronous way so that it will not block for network
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
     * @param componentId
     *            ID of the component for which all data items should be
     *            retrieved.
     * @return all data items for the specified component exposed through a
     *         future. In case of failure, the future's <code>get()</code>
     *         method will throw an exception.
     */
    ListenableFuture<? extends Iterable<? extends DataItem>> getAllItems(
            UUID componentId);

    /**
     * Returns the data item with the specified key for the specified component.
     * The operation is performed in an asynchronous way so that it will not
     * block for network communication. The result of the operation can be
     * checked through the returned future.
     * 
     * @param componentId
     *            ID of the component that owns the data item.
     * @param key
     *            key identifying the data item within the component.
     * @return data item with the specified key for the specified component
     *         exposed through a future. If no such data item exists, the
     *         future's <code>get</code> method returns <code>null</code>. In
     *         case of failure, the future's <code>get()</code> method will
     *         throw an exception.
     */
    ListenableFuture<? extends DataItem> getItem(UUID componentId, String key);

    /**
     * Tells whether this DAO has finished initialization and is ready to be
     * used. Even if this method returns <code>true</code>, operations might
     * still fail because of transient problems.
     * 
     * @return <code>true</code> if this DAO has been initialized and is ready
     *         to be used, <code>false</code> if initialization has not
     *         completed yet.
     */
    boolean isInitialized();

    /**
     * Updates the data item with the specified key and component. The item is
     * only updated if its current value matches the specified
     * <code>oldValue</code>. This type of atomic operation incurs an additional
     * overhead. Code that does not mind overwriting a different value should
     * prefer the {@link #createOrUpdateItem(UUID, String, String)} method. The
     * operation is performed in an asynchronous way so that it will not block
     * for network communication. The result of the operation can be checked
     * through the returned future.
     * 
     * @param componentId
     *            ID of the component that owns the data item.
     * @param key
     *            key identifying the data item within the component.
     * @param oldValue
     *            old value of the data item that must be matched for the update
     *            operation to proceed.
     * @param newValue
     *            new value that the data item shall be updated with if the old
     *            value matches.
     * @return pair of a boolean flag and a string exposed through a future. The
     *         boolean flag indicates whether the item was updated. If
     *         <code>true</code>, the item was updated, if <code>false</code>,
     *         the item's value did not match the specified
     *         <code>oldValue</code> or the item did not exist. If the flag is
     *         <code>false</code>, the string is the current value of the
     *         existing item or <code>null</code> if no item with the specified
     *         component ID and key exists. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Pair<Boolean, String>> updateItem(UUID componentId,
            String key, String oldValue, String newValue);

}
