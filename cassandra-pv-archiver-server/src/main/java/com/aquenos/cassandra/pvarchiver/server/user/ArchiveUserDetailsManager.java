/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.user;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.provisioning.UserDetailsManager;

import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAO;
import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAO.DataItem;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Manager for archive users. This class acts as a {@link UserDetailsService}
 * that can be used as part of the authentication system and allows modifying
 * the user information in the database through the {@link UserDetailsManager}
 * interface.
 * </p>
 * 
 * <p>
 * When used with authentication methods that result in the
 * {@link #loadUserByUsername(String)} method being called frequently (e.g. each
 * request), it is recommended to add a <code>UserCache</code> to the
 * <code>AuthenticationProvider</code> in order to reduce latency.
 * </p>
 * 
 * <p>
 * Internally, this service uses the {@link GenericDataStoreDAO} for storing
 * user information.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ArchiveUserDetailsManager implements InitializingBean,
        UserDetailsManager {

    /**
     * Class representing the payload of a user details object. Basically, this
     * is the complete user details object, but without the username. This class
     * is only used internally for serializing to and deserialzing from the JSON
     * representation in the database.
     * 
     * @author Sebastian Marsching
     */
    private static class JsonUserDetailsPayload {

        private final String passwordHashBCrypt;
        private final Set<String> roles;

        public JsonUserDetailsPayload(
                @JsonProperty("passwordHashBCrypt") String passwordHashBCrypt,
                @JsonProperty("roles") Set<String> roles) {
            if (roles == null) {
                roles = Collections.emptySet();
            }
            if (passwordHashBCrypt == null) {
                passwordHashBCrypt = "";
            }
            this.roles = roles;
            this.passwordHashBCrypt = passwordHashBCrypt;
        }

        public String getPasswordHashBCrypt() {
            return passwordHashBCrypt;
        }

        public Set<String> getRoles() {
            return roles;
        }

    }

    private static final String ROLE_AUTHORITY_PREFIX = "ROLE_";

    /**
     * String identifying the role associated with administrative privileges.
     * 
     * @see #ROLE_ADMIN_AUTHORITY
     */
    public static final String ROLE_ADMIN = "ADMIN";

    /**
     * Authority associated with administrative privileges. This authority is
     * represented by the string "ROLE_ADMIN".
     */
    private static final GrantedAuthority ROLE_ADMIN_AUTHORITY = new SimpleGrantedAuthority(
            ROLE_AUTHORITY_PREFIX + ROLE_ADMIN);

    /**
     * Component identifier for this service used when accessing the
     * {@link GenericDataStoreDAO}. This identifier is only intended for
     * internal use, but it is package-private so that test code associated with
     * this class can access it.
     */
    static final UUID GENERIC_DATA_STORE_COMPONENT_ID = UUID
            .fromString("ad5e517b-4ab6-4c4e-8eed-5d999de7484f");

    private static final String ADMIN_USER_NAME = "admin";
    // The default password hash is for the password "admin".
    private static final String ADMIN_USER_DEFAULT_PASSWORD_HASH = "$2a$10$pCni7piWut4sGHPp6CipGeutqN8UCJp0sJDCZHK.Iwetdvibsn9Li";

    private GenericDataStoreDAO genericDataStoreDAO;
    private ObjectMapper objectMapper;
    private ObjectReader objectReaderAllowUnknownProperties;
    private ObjectReader objectReaderStrict;
    private ObjectWriter objectWriter;
    private BCryptPasswordEncoder passwordEncoder;

    @Override
    public void afterPropertiesSet() throws Exception {
        // If no object mapper is specified explicitly, we build one using a
        // standard configuration.
        if (objectMapper == null) {
            // We enabled the fail-on-null-for-primitives features because a
            // null value that represents a primitive cannot be right and
            // indicates that something is wrong with the serialized data.
            objectMapper = Jackson2ObjectMapperBuilder
                    .json()
                    .failOnUnknownProperties(false)
                    .featuresToEnable(
                            DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                    .build();
        }
        // We create two kinds of object readers: one that allows unknown
        // properties and one that does not. We use the first one when
        // authenticating users so that users that have been created or modified
        // by more recent versions of this software and that might have
        // additional properties can still be authenticated with this server.
        // The second one is used in contrast when we have to modify a user
        // stored in the database. In this case we have to ensure that we read
        // all data and will not accidentally drop any properties when writing
        // the updated data.
        ObjectReader baseReader = objectMapper
                .readerFor(JsonUserDetailsPayload.class);
        objectReaderAllowUnknownProperties = baseReader
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectReaderStrict = baseReader
                .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectWriter = objectMapper.writer();
        // If the password encoder has not been specified explicitly, we create
        // one that uses the default options
        if (passwordEncoder == null) {
            passwordEncoder = new BCryptPasswordEncoder();
        }
    }

    @Override
    public void changePassword(String oldPlainTextPassword,
            String newPlainTextPassword) {
        Preconditions.checkNotNull(oldPlainTextPassword,
                "The old password must not be null.");
        Preconditions.checkNotNull(newPlainTextPassword,
                "The new password must not be null.");
        Preconditions.checkArgument(!newPlainTextPassword.isEmpty(),
                "The new password must not be empty.");
        Authentication currentUser = SecurityContextHolder.getContext()
                .getAuthentication();
        if (currentUser == null) {
            throw new AccessDeniedException(
                    "Can't change password as no Authentication object found in context "
                            + "for current user.");
        }
        String username = currentUser.getName();
        Preconditions.checkArgument(!username.isEmpty(),
                "The username must not be empty.");
        DataItem dataItem = loadDataItemByUsername(username);
        String oldPayloadString = (dataItem == null) ? null : dataItem
                .getValue();
        JsonUserDetailsPayload oldPayload;
        try {
            oldPayload = (oldPayloadString == null) ? null
                    : convertJsonStringToJsonUserDetailsPayload(
                            oldPayloadString, objectReaderStrict);
        } catch (RuntimeException e1) {
            // It could be that the payload in the database has additional
            // properties that we do not understand. In this case, reading with
            // the non-strict reader should succeed.
            try {
                convertJsonStringToJsonUserDetailsPayload(oldPayloadString,
                        objectReaderStrict);
                // If we got here, we successfully read the payload with the
                // non-strict reader. This means that we can wrap the original
                // exception in a suitable exception giving more details.
                throw new RuntimeException(
                        "The user information in the database contains additional details that are not supported by this version of the software. Most likely, the user in the database was created or modified by a newer version of this software.",
                        e1);
            } catch (RuntimeException e2) {
                // If we also get an exception with the non-strict reader, the
                // problem must have a different cause and we simply rethrow the
                // original exception.
                throw e1;
            }
        }
        // Even if the old payload is null, we might get user details if the
        // user is the special admin user that always exists.
        UserDetails userDetails = convertJsonUserDetailsPayloadToUserDetails(
                username, oldPayload);
        if (userDetails == null) {
            throw new UsernameNotFoundException(
                    "The specified username does not exist.");
        }
        if (!passwordEncoder.matches(oldPlainTextPassword,
                userDetails.getPassword())) {
            throw new BadCredentialsException(
                    "The specified old password does not match the stored password.");
        }
        String newPayloadString;
        try {
            newPayloadString = objectWriter
                    .writeValueAsString(new JsonUserDetailsPayload(
                            passwordEncoder.encode(newPlainTextPassword),
                            (oldPayload == null) ? Collections
                                    .singleton(ROLE_ADMIN) : oldPayload
                                    .getRoles()));
        } catch (JsonProcessingException e) {
            // Typically this should not happen unless there is something wrong
            // with our ObjectMapper.
            throw new RuntimeException(
                    "Unexpected error while trying to serialize user details to JSON.",
                    e);
        }
        if (dataItem == null) {
            // It is possible that the default admin user does not exist in the
            // database yet. In this case, we create it.
            Pair<Boolean, String> result = FutureUtils
                    .getUnchecked(genericDataStoreDAO.createItem(
                            GENERIC_DATA_STORE_COMPONENT_ID, username,
                            newPayloadString));
            if (!result.getLeft()) {
                // The user was created concurrently. We simply call this method
                // again so that it can handle the situation where the user
                // already exists.
                changePassword(oldPlainTextPassword, newPlainTextPassword);
                return;
            }
        } else {
            // The user already exists, so we have to update the database
            // record.
            Pair<Boolean, String> result = FutureUtils
                    .getUnchecked(genericDataStoreDAO.updateItem(
                            GENERIC_DATA_STORE_COMPONENT_ID, username,
                            oldPayloadString, newPayloadString));
            if (!result.getLeft()) {
                // The user was changed concurrently. We simply call this method
                // again so that it can use the updated information.
                changePassword(oldPlainTextPassword, newPlainTextPassword);
                return;
            }
        }
    }

    @Override
    public void createUser(UserDetails user) {
        Preconditions.checkNotNull(user,
                "The user details object must not be null.");
        String username = user.getUsername();
        Preconditions.checkArgument(!username.isEmpty(),
                "The username must not be empty.");
        String passwordHash = user.getPassword();
        Preconditions.checkNotNull(passwordHash,
                "The password hash must not be null.");
        Preconditions.checkArgument(!passwordHash.isEmpty(),
                "The password must not be empty.");
        String payload;
        try {
            payload = objectWriter
                    .writeValueAsString(new JsonUserDetailsPayload(
                            passwordHash, convertAuthoritiesToRoles(user
                                    .getAuthorities())));
        } catch (JsonProcessingException e) {
            // Typically this should not happen unless there is something wrong
            // with our ObjectMapper.
            throw new RuntimeException(
                    "Unexpected error while trying to serialize user details to JSON.",
                    e);
        }
        Pair<Boolean, String> result = FutureUtils
                .getUnchecked(genericDataStoreDAO.createItem(
                        GENERIC_DATA_STORE_COMPONENT_ID, username, payload));
        if (!result.getLeft()) {
            throw new IllegalArgumentException(
                    "A user with the specified name already exists in the database.");
        }
    }

    @Override
    public void deleteUser(String username) {
        Preconditions.checkNotNull(username, "The username must not be null.");
        Preconditions.checkArgument(!username.equals(ADMIN_USER_NAME),
                "The admin user may not be removed.");
        FutureUtils.getUnchecked(genericDataStoreDAO.deleteItem(
                GENERIC_DATA_STORE_COMPONENT_ID, username));
    }

    @Override
    public UserDetails loadUserByUsername(String username)
            throws UsernameNotFoundException {
        Preconditions.checkNotNull(username);
        DataItem dataItem = loadDataItemByUsername(username);
        String payloadString = (dataItem == null) ? null : dataItem.getValue();
        UserDetails userDetails = convertJsonUserDetailsPayloadToUserDetails(
                username,
                convertJsonStringToJsonUserDetailsPayload(payloadString,
                        objectReaderAllowUnknownProperties));
        if (userDetails == null) {
            throw new UsernameNotFoundException(
                    "The specified username does not exist.");
        } else {
            return userDetails;
        }
    }

    /**
     * Sets the DAO for reading and writing generic pieces of configuration
     * data. In particular, this class uses the DAO for reading and writing the
     * user objects. Typically, this method is called automatically by the
     * Spring container.
     * 
     * @param genericDataStoreDAO
     *            generic data-store DAO to be used by this service.
     */
    @Autowired
    public void setGenericDataStoreDAO(GenericDataStoreDAO genericDataStoreDAO) {
        this.genericDataStoreDAO = genericDataStoreDAO;
    }

    /**
     * Sets the object mapper used for converting between the JSON and the Java
     * representation of user objects. If not set explicitly, an object mapper
     * using a default configuration suitable for this purpose is created.
     * 
     * @param objectMapper
     *            object mapper used for serializing user objects to and
     *            deserializing user objects from their JSON representation,
     *            which is used when storing them in the database.
     */
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Sets the password encoded used for encoding and checking password. This
     * encoder is used when calling the {@link #changePassword(String, String)}
     * method. All other methods only deal with password hashes so that they do
     * not need a password encoder. If not set explicitly, a BCrypt password
     * encoder is created using the default constructor.
     * 
     * @param passwordEncoder
     *            password encoder to be used for checking the old and
     *            generating the new password hash in
     *            {@link #changePassword(String, String)}.
     */
    public void setPasswordEncoder(BCryptPasswordEncoder passwordEncoder) {
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public void updateUser(UserDetails user) {
        Preconditions.checkNotNull(user);
        String username = user.getUsername();
        DataItem dataItem = loadDataItemByUsername(username);
        String oldPayloadString = (dataItem == null) ? null : dataItem
                .getValue();
        JsonUserDetailsPayload oldPayload;
        try {
            oldPayload = (oldPayloadString == null) ? null
                    : convertJsonStringToJsonUserDetailsPayload(
                            oldPayloadString, objectReaderStrict);
        } catch (RuntimeException e1) {
            // It could be that the payload in the database has additional
            // properties that we do not understand. In this case, reading with
            // the non-strict reader should succeed.
            try {
                convertJsonStringToJsonUserDetailsPayload(oldPayloadString,
                        objectReaderStrict);
                // If we got here, we successfully read the payload with the
                // non-strict reader. This means that we can wrap the original
                // exception in a suitable exception giving more details.
                throw new RuntimeException(
                        "The user information in the database contains additional details that are not supported by this version of the software. Most likely, the user in the database was created or modified by a newer version of this software.",
                        e1);
            } catch (RuntimeException e2) {
                // If we also get an exception with the non-strict reader, the
                // problem must have a different cause and we simply rethrow the
                // original exception.
                throw e1;
            }
        }
        // Even if the old payload is null, we might get user details if the
        // user is the special admin user that always exists.
        UserDetails oldUser = convertJsonUserDetailsPayloadToUserDetails(
                username, oldPayload);
        if (oldUser == null) {
            throw new UsernameNotFoundException(
                    "The specified username does not exist.");
        }
        String newPayloadString;
        try {
            newPayloadString = objectWriter
                    .writeValueAsString(new JsonUserDetailsPayload(user
                            .getPassword(), convertAuthoritiesToRoles(user
                            .getAuthorities())));
        } catch (JsonProcessingException e) {
            // Typically this should not happen unless there is something wrong
            // with our ObjectMapper.
            throw new RuntimeException(
                    "Unexpected error while trying to serialize user details to JSON.",
                    e);
        }
        if (dataItem == null) {
            // It is possible that the default admin user does not exist in the
            // database yet. In this case, we create it.
            Pair<Boolean, String> result = FutureUtils
                    .getUnchecked(genericDataStoreDAO.createItem(
                            GENERIC_DATA_STORE_COMPONENT_ID, username,
                            newPayloadString));
            if (!result.getLeft()) {
                // The user was created concurrently. We simply call this method
                // again so that it can handle the situation where the user
                // already exists.
                updateUser(user);
                return;
            }
        } else {
            // The user already exists, so we have to update the database
            // record.
            Pair<Boolean, String> result = FutureUtils
                    .getUnchecked(genericDataStoreDAO.updateItem(
                            GENERIC_DATA_STORE_COMPONENT_ID, username,
                            oldPayloadString, newPayloadString));
            if (!result.getLeft()) {
                // The user was changed concurrently. We simply call this method
                // again so that it can use the updated information.
                updateUser(user);
                return;
            }
        }
    }

    @Override
    public boolean userExists(String username) {
        try {
            loadUserByUsername(username);
            return true;
        } catch (UsernameNotFoundException e) {
            return false;
        }
    }

    private SortedSet<String> convertAuthoritiesToRoles(
            Collection<? extends GrantedAuthority> authorities) {
        // We only support authorities that represent a role. These authorities
        // are identified by a string that starts with "ROLE_" followed by the
        // name of the role.
        TreeSet<String> roles = new TreeSet<String>();
        for (GrantedAuthority authority : authorities) {
            String authorityString = authority.getAuthority();
            if (authorityString == null) {
                throw new IllegalArgumentException(
                        "The user details object contained an authority that cannot be represented by a string.");
            }
            String authorityStringUpperCase = authorityString
                    .toUpperCase(Locale.ENGLISH);
            if (!authorityStringUpperCase.startsWith(ROLE_AUTHORITY_PREFIX)) {
                throw new IllegalArgumentException(
                        "Only authorities representing a role (identified by a string starting with \"ROLE_\") are supported.");
            }
            String role = authorityStringUpperCase
                    .substring(ROLE_AUTHORITY_PREFIX.length());
            if (roles.contains(role)) {
                throw new IllegalArgumentException(
                        "The authority \""
                                + StringEscapeUtils.escapeJava(authorityString)
                                + "\" has been specified more than once. Possibly two different roles have been mapped to the same string because the string comparison is not case sensitive.");
            }
            roles.add(role);
        }
        return Collections.unmodifiableSortedSet(roles);
    }

    private Collection<? extends GrantedAuthority> convertRolesToAuthorities(
            Set<String> roles) {
        // The authority is created from a role by adding the ROLE_ prefix to
        // the role name.
        HashSet<GrantedAuthority> authorities = new HashSet<GrantedAuthority>();
        for (String role : roles) {
            role = role.toUpperCase(Locale.ENGLISH);
            switch (role) {
            case ROLE_ADMIN:
                authorities.add(ROLE_ADMIN_AUTHORITY);
                break;
            default:
                authorities.add(new SimpleGrantedAuthority(
                        ROLE_AUTHORITY_PREFIX + role));
                break;
            }
        }
        return Collections.unmodifiableSet(authorities);
    }

    private JsonUserDetailsPayload convertJsonStringToJsonUserDetailsPayload(
            String jsonString, ObjectReader objectReader) {
        if (jsonString == null) {
            return null;
        }
        try {
            return objectReader.<JsonUserDetailsPayload> readValue(jsonString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Error while trying to deserialize user details from JSON form: "
                            + e.getMessage(), e);
        } catch (IOException e) {
            // There is really no reason why we should expect such an exception
            // when parsing a string.
            throw new RuntimeException(
                    "Unexpected I/O exception while trying deserialize user details.",
                    e);
        }
    }

    private UserDetails convertJsonUserDetailsPayloadToUserDetails(
            String username, JsonUserDetailsPayload payload) {
        assert (username != null);
        if (payload == null) {
            // If the user is the special admin user, we provide a default
            // user-details object if the user is not present in the database.
            // We return a new instance every time because it is mutable and the
            // password hash might be erased by code using it.
            if (username.equals(ADMIN_USER_NAME)) {
                return new User(ADMIN_USER_NAME,
                        ADMIN_USER_DEFAULT_PASSWORD_HASH,
                        Collections.singleton(ROLE_ADMIN_AUTHORITY));
            } else {
                return null;
            }
        } else {
            return new User(username, payload.getPasswordHashBCrypt(),
                    convertRolesToAuthorities(payload.getRoles()));
        }
    }

    private DataItem loadDataItemByUsername(String username) {
        assert (username != null);
        return FutureUtils.getUnchecked(genericDataStoreDAO.getItem(
                GENERIC_DATA_STORE_COMPONENT_ID, username));
    }

}
