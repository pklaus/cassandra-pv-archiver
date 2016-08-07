/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.event.EventListener;
import org.springframework.util.Base64Utils;

import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAO;
import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAO.DataItem;
import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAOInitializedEvent;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Authentication services for the inter-node communication.
 * </p>
 * 
 * <p>
 * On startup, this service reads a shared secret from the Cassandra database.
 * If the shared secret is not found, it generates a new one and stores it in
 * the database for use by other servers. When this initialization has finished,
 * an {@link InterNodeCommunicationAuthenticationServiceInitializedEvent} is
 * published through the application event publisher. The initialization status
 * can also be queried with the {@link #isInitialized()} method.
 * </p>
 * 
 * <p>
 * When the {@link #getUsernameAndPassword(Role)} method is called, this service
 * uses the shared secret to calculate an HMAC on the current time. This HMAC
 * and the current time are combined and returned as a password. This password
 * can then be used for authentication with other nodes in the cluster.
 * </p>
 * 
 * <p>
 * When a server in the cluster receives a request from another server, it can
 * verify the credentials provided by that server using the
 * {@link #authenticate(String, String)} method. This method verifies, that the
 * HMAC presented as part of the password was actually generated using the same
 * shared secret. It also checks that the provided time-stamp is not too far in
 * the past or the future. This way, even if an attacker gets knowledge of the
 * password, it is only useful for a limited amount of time. The shared secret
 * itself is never transported over the network (except for communication with
 * the Cassandra database which has to be protected separately).
 * </p>
 * 
 * <p>
 * The authentication mechanism implemented by this class is not secure if used
 * over an untrusted network because it does not enforce any authentication on
 * the content of a request and - in addition to that - is vulnerable to replay
 * attacks. The authentication mechanism is not designed to provide protection
 * from an attacker that acts as a man-in-the-middle. However, it can provide
 * sufficient protection against accidental connections from other servers that
 * do not belong to the same cluster but have been misconfigured to contact
 * servers that are not part of their cluster.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class InterNodeCommunicationAuthenticationService implements
        ApplicationEventPublisherAware, SmartInitializingSingleton {

    /**
     * <p>
     * Role for which a client authenticated. The role defines the kind of
     * resources the client may access.
     * </p>
     * 
     * <p>
     * In general, the {@link #PRIVILEGED} role is used for inter-node
     * communication. However, this role depends on an authentication token that
     * involves a time stamp. Therefore, authentication on this level might not
     * work if there is a significant clock skew between servers. For
     * applications where communication should still be possible under such
     * circumstances, the {@link #UNPRIVILEGED} role can be used. This role uses
     * a fixed token. Obviously, this role should never be used for sensitive
     * services because an attacker gaining access to the shared secret once
     * (e.g. by listening on the network wire) will be able to authenticate with
     * this role indefinitely.
     * </p>
     * 
     * @author Sebastian Marsching
     */
    public static enum Role {
        /**
         * Authentication was not successful.
         */
        NOT_AUTHENTICATED,

        /**
         * Authentication for unprivileged access. Unprivileged access means
         * that only resources that do not affect the operation may be accessed
         * (e.g. read-only access to insensitive data).
         */
        UNPRIVILEGED,

        /**
         * Authentication for privileged access. Privileged access means that
         * sensitive resources may be accessed.
         */
        PRIVILEGED

    }

    private static final String COMPONENT_ID_STRING = "e2ea658a-4d4e-4d0d-8e01-5cd0896ac245";
    private static final UUID COMPONENT_ID = UUID
            .fromString(COMPONENT_ID_STRING);
    private static final long INITIALIZATION_DELAY_ON_ERROR_MILLISECONDS = 60000L;
    private static final String MAC_ALGORITHM = "HmacSHA256";
    private static final int MAC_SIZE;

    static {
        try {
            Mac mac = Mac.getInstance(MAC_ALGORITHM);
            MAC_SIZE = mac.getMacLength();
        } catch (NoSuchAlgorithmException e) {
            // This should never happen because HmacSHA256 is guaranteed to
            // be supported by the Java platform.
            throw new RuntimeException("Unexpected error: MAC algorithm "
                    + MAC_ALGORITHM + " is not supported: " + e.getMessage(), e);
        }
    }

    private static final long MAX_CLOCK_SKEW_MILLISECONDS = 5000L;
    private static final String SHARED_SECRET_KEY = "shared_secret";
    private static final int TIME_STAMP_SIZE = Long.SIZE / 8;
    private static final String USERNAME_PRIVILEGED = "inter-node-communication-privileged-1.0";
    private static final String USERNAME_UNPRIVILEGED = "inter-node-communication-unprivileged-1.0";

    // PADDING_SIZE needs to be declared after MAC_SIZE and TIME_STAMP_SIZE. We
    // use padding so that the total number of bytes in the password string is
    // divisible by three which results in a nicer encoded string.
    private static final int PADDING_SIZE = (((MAC_SIZE + TIME_STAMP_SIZE) % 3) == 0) ? 0
            : (3 - ((MAC_SIZE + TIME_STAMP_SIZE) % 3));

    private static String bytesToString(byte[] bytes) {
        return Base64Utils.encodeToString(bytes);
    }

    private static byte[] stringToBytes(String string) {
        return Base64Utils.decodeFromString(string);
    }

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private ApplicationEventPublisher applicationEventPublisher;
    private GenericDataStoreDAO genericDataStoreDAO;
    private Function<Throwable, Void> handleDatabaseError = new Function<Throwable, Void>() {
        @Override
        public Void apply(Throwable input) {
            // The most likely reason for the operation to fail is
            // that the Cassandra database is not available. We
            // simply try again later.
            final Timer timer = new Timer(true);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    timer.cancel();
                    initializeSharedSecret();
                }
            }, INITIALIZATION_DELAY_ON_ERROR_MILLISECONDS);
            // We also log the exception in case someone is interested in it.
            log.error(
                    "Error while trying to initialize the shared secret for the inter-node communication service. The operation will be tried again in "
                            + INITIALIZATION_DELAY_ON_ERROR_MILLISECONDS
                            + " ms.", input);
            return null;
        }
    };
    private AtomicBoolean initializeSharedSecretStarted = new AtomicBoolean();
    // The shared secret must be volatile because we want to read it from a
    // different thread than the one that initializes it and initialization will
    // complete asynchronously.
    private volatile SecretKey sharedSecret;

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Sets the DAO for reading and writing generic pieces of configuration
     * data. In particular, this class uses the DAO for reading and writing the
     * shared secret that is used for the authentication mechanism. Typically,
     * this method is called automatically by the Spring container.
     * 
     * @param genericDataStoreDAO
     *            generic data-store DAO to be used by this service.
     */
    @Autowired
    public void setGenericDataStoreDAO(GenericDataStoreDAO genericDataStoreDAO) {
        this.genericDataStoreDAO = genericDataStoreDAO;
    }

    /**
     * Handles {@link GenericDataStoreDAOInitializedEvent}s. This event is used
     * when the {@link GenericDataStoreDAO} set using
     * {@link #setGenericDataStoreDAO(GenericDataStoreDAO)} is not ready yet
     * when this object is initialized. In this case, initialization code that
     * depends on the generic data-store DAO is delayed until the DAO has
     * finished initialization.
     * 
     * @param event
     *            initialization event sent by the {@link GenericDataStoreDAO}.
     */
    @EventListener
    public void onGenericDataStoreDAOInitializedEvent(
            GenericDataStoreDAOInitializedEvent event) {
        if (event.getSource() != this.genericDataStoreDAO) {
            return;
        }
        startInitializeSharedSecret();
    }

    @Override
    public void afterSingletonsInstantiated() {
        // We might have missed the event, so we have to make sure that we
        // initialize the shared secret if the generic data-store DAO has
        // already been fully initialized.
        if (genericDataStoreDAO.isInitialized()) {
            startInitializeSharedSecret();
        }
    }

    /**
     * <p>
     * Tries to authenticate a request with the specified username and password.
     * The username needs to be the same username that is returned by
     * {@link #getUsernameAndPassword(Role)}. The password is validated
     * according to the following rules:
     * </p>
     * 
     * <p>
     * The password is decoded into a time stamp and a corresponding MAC. If the
     * total number of the decoded bytes is not equal to the number of expected
     * bytes, the authentication fails.
     * </p>
     * 
     * <p>
     * The MAC of the time stamp specified as part of the password is calculated
     * using the internal shared secret. If the calculated MAC does not match
     * the MAC provided as part of the password, the authentication fails.
     * </p>
     * 
     * <p>
     * Finally, the provided time-stamp is checked. If it is too far in the past
     * or future (more than five seconds in each direction), the authentication
     * fails. Otherwise, the authentication is succeeds.
     * </p>
     * 
     * <p>
     * In general, the authentication should succeed if the supplied username /
     * password pair has been recently generated using the
     * {@link #getUsernameAndPassword(Role)} method of an instance internally
     * using the same shared secret. However, if the generating instance runs on
     * a different host than the validating one, the clocks need to be
     * synchronized within a few seconds so that the time-stamp presented as
     * part of the password is considered valid.
     * </p>
     * 
     * <p>
     * For authentication with the {@link Role#UNPRIVILEGED} role, the clock
     * skew does not matter.
     * </p>
     * 
     * @param username
     *            username provided by the client.
     * @param password
     *            password provided by the client.
     * @return {@link Role#PRIVILEGED} if the client was authenticated for
     *         privileged access, {@link Role#UNPRIVILEGED} if the client was
     *         authenticated for unprivileged access and
     *         {@link Role#NOT_AUTHENTICATED} if the client was not
     *         authenticated at all.
     * @throws IllegalStateException
     *             if the authentication cannot complete because this service
     *             has not initialized completely yet. Typically, this happens
     *             if this method is called before the connection with the
     *             Cassandra database has been established.
     * @throws NullPointerException
     *             if <code>username</code> or <code>password</code> are
     *             <code>null</code>.
     * @see #getUsernameAndPassword(Role)
     */
    public Role authenticate(String username, String password) {
        Preconditions.checkNotNull(username, "The username must no be null.");
        Preconditions.checkNotNull(password, "The password must not be null.");
        if (sharedSecret == null) {
            throw new IllegalStateException(
                    "Authentication cannot continue because the shared secret is not available yet.");
        }
        byte[] passwordDecoded;
        try {
            passwordDecoded = stringToBytes(password);
        } catch (IllegalArgumentException e) {
            // If the password is malformed (not a valid encoded string), the
            // authentication fails.
            return Role.NOT_AUTHENTICATED;
        }
        if (passwordDecoded.length != TIME_STAMP_SIZE + MAC_SIZE + PADDING_SIZE) {
            // If the size does not match, this cannot be a correct
            // authentication request.
            return Role.NOT_AUTHENTICATED;
        }
        ByteBuffer timeStampBuffer = ByteBuffer.wrap(passwordDecoded, 0,
                TIME_STAMP_SIZE);
        byte[] actualMac = Arrays.copyOfRange(passwordDecoded, TIME_STAMP_SIZE,
                TIME_STAMP_SIZE + MAC_SIZE);
        long timeStamp = timeStampBuffer.getLong();
        timeStampBuffer.position(0);
        byte[] expectedMac = calculateMac(timeStampBuffer);
        if (!Arrays.equals(expectedMac, actualMac)) {
            // If the MAC specified in the password is invalid, the
            // authentication fails.
            return Role.NOT_AUTHENTICATED;
        }
        long currentTime = System.currentTimeMillis();
        if (timeStamp == 0L && username.equals(USERNAME_UNPRIVILEGED)) {
            return Role.UNPRIVILEGED;
        }
        if (timeStamp < currentTime - MAX_CLOCK_SKEW_MILLISECONDS
                || timeStamp > currentTime + MAX_CLOCK_SKEW_MILLISECONDS) {
            // If the time-stamp provided by the client is outside the accepted
            // range, the authentication information is not up-to-date (or the
            // clock skew between two machines is too large), so the
            // authentication fails.
            return Role.NOT_AUTHENTICATED;
        }
        // We run the test of the username last. In our case (fixed username) it
        // does not matter, but in general it is a good idea to avoid giving an
        // attacker information about whether the username or the password was
        // wrong (not even based on timing) and one never knows where this code
        // might end up eventually.
        if (!username.equals(USERNAME_PRIVILEGED)) {
            // If the username does not match, the authentication fails.
            return Role.NOT_AUTHENTICATED;
        }
        return Role.PRIVILEGED;
    }

    /**
     * <p>
     * Returns a username / password pair that is suitable for authentication
     * with this service. In general, if the username / password pair returned
     * by this method is passed to {@link #authenticate(String, String)}, that
     * method will return <code>true</code>.
     * </p>
     * 
     * <p>
     * This will work across different instances of this class (even across
     * different JVMs or hosts) if the shared secret used by this service is the
     * same. For the privileged role, the password returned by this method is
     * only valid for a limited amount of time. Therefore, authentication can
     * fail if too much time passed between calling this method and
     * {@link #authenticate(String, String)} or if
     * {@link #authenticate(String, String)} is called on a different host with
     * a clock that is not sufficiently synchronized with the clock on this
     * host.
     * </p>
     * 
     * @param role
     *            role for which the credentials shall be provided. When the
     *            returned credentials are passed to the
     *            {@link #authenticate(String, String)} method, it will return
     *            the specified role. Must not be {@link Role#NOT_AUTHENTICATED}
     *            .
     * @return pair storing the username as the first and the password as the
     *         second value. The username and password are suitable for use with
     *         the <code>authenticate(...)</code> method.
     * @throws IllegalArgumentException
     *             if <code>role</code> is {@link Role#NOT_AUTHENTICATED}.
     * @throws IllegalStateException
     *             if the password cannot be generated because this service has
     *             not initialized completely yet. Typically, this happens if
     *             this method is called before the connection with the
     *             Cassandra database has been established.
     * @throws NullPointerException
     *             if <code>role</code> is <code>null</code>.
     * @see #authenticate(String, String)
     */
    public Pair<String, String> getUsernameAndPassword(Role role) {
        Preconditions.checkNotNull(role, "The role must not be null.");
        Preconditions.checkArgument(!role.equals(Role.NOT_AUTHENTICATED),
                "The role must be PRIVILEGED or UNPRIVILEGED.");
        if (sharedSecret == null) {
            throw new IllegalStateException(
                    "Password generation cannot continue because the shared secret is not available yet.");
        }
        long timeStamp;
        String username;
        switch (role) {
        case UNPRIVILEGED:
            timeStamp = 0L;
            username = USERNAME_UNPRIVILEGED;
            break;
        case PRIVILEGED:
            timeStamp = System.currentTimeMillis();
            username = USERNAME_PRIVILEGED;
            break;
        default:
            throw new RuntimeException("Logic error: Got unhandled role of "
                    + role + ".");
        }
        byte[] timeStampBytes = new byte[TIME_STAMP_SIZE];
        ByteBuffer timeStampBuffer = ByteBuffer.wrap(timeStampBytes);
        timeStampBuffer.putLong(timeStamp);
        timeStampBuffer.flip();
        byte[] mac = calculateMac(timeStampBuffer);
        byte[] passwordBytes = new byte[TIME_STAMP_SIZE + MAC_SIZE
                + PADDING_SIZE];
        System.arraycopy(timeStampBytes, 0, passwordBytes, 0, TIME_STAMP_SIZE);
        System.arraycopy(mac, 0, passwordBytes, TIME_STAMP_SIZE, MAC_SIZE);
        return Pair.of(username, bytesToString(passwordBytes));
    }

    /**
     * <p>
     * Tells whether this authentication service has initialized completely and
     * is ready for operation. The authentication service is ready as soon as it
     * has retrieved the shared secret used for inter-node authentication from
     * the database or has generated a shared secret and successfully stored it
     * in the database. The latter case only happens if there is no shared
     * secret in the database yet.
     * </p>
     * 
     * <p>
     * When this service changes to the initialized state, it publishes an
     * {@link InterNodeCommunicationAuthenticationServiceInitializedEvent} to
     * the application context.
     * </p>
     * 
     * @return <code>true</code> if initialization has completed and this
     *         authentication service can be used, <code>false</code> otherwise.
     */
    public boolean isInitialized() {
        return sharedSecret != null;
    }

    private byte[] calculateMac(ByteBuffer input) {
        Mac mac;
        try {
            mac = Mac.getInstance(MAC_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            // This should never happen because HmacSHA256 is guaranteed to
            // be supported by the Java platform.
            throw new RuntimeException("Unexpected error: MAC algorithm "
                    + MAC_ALGORITHM + " is not supported: " + e.getMessage(), e);
        }
        try {
            mac.init(sharedSecret);
        } catch (InvalidKeyException e) {
            // This should never happen because HmacSHA256 is guaranteed to
            // be supported by the Java platform and we generated the key
            // specifically for this algorithm.
            throw new RuntimeException(
                    "Unexpected error: MAC did not accept the key generated for it: "
                            + e.getMessage(), e);
        }
        mac.update(input);
        byte[] result = mac.doFinal();
        assert (result.length == MAC_SIZE);
        return result;
    }

    private void generateSharedSecret() {
        KeyGenerator keyGenerator;
        try {
            keyGenerator = KeyGenerator.getInstance(MAC_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            // This should never happen because HmacSHA256 is guaranteed to be
            // supported by the Java platform.
            log.error("Unexpected error: MAC algorithm " + MAC_ALGORITHM
                    + " is not supported.", e);
            return;
        }
        final SecretKey newSharedSecret = keyGenerator.generateKey();
        FutureUtils.transform(
                genericDataStoreDAO.createItem(COMPONENT_ID, SHARED_SECRET_KEY,
                        bytesToString(newSharedSecret.getEncoded())),
                new Function<Pair<Boolean, String>, Void>() {
                    @Override
                    public Void apply(Pair<Boolean, String> input) {
                        // If we could store the generated shared secret, we can
                        // use it from now on. If we could not store it, we got
                        // the value that is already stored and we can use that
                        // value.
                        if (input.getLeft()) {
                            sharedSecret = newSharedSecret;
                        } else {
                            sharedSecret = new SecretKeySpec(
                                    stringToBytes(input.getRight()),
                                    MAC_ALGORITHM);
                        }
                        sharedSecretInitialized();
                        return null;
                    }
                }, handleDatabaseError);
    }

    private void initializeSharedSecret() {
        FutureUtils.transform(
                genericDataStoreDAO.getItem(COMPONENT_ID, SHARED_SECRET_KEY),
                new Function<DataItem, Void>() {
                    @Override
                    public Void apply(DataItem input) {
                        if (input == null) {
                            // If the shared secret has not been stored yet, we
                            // have to generate and store it. We do this in a
                            // separate thread because - in the worst case -
                            // the generation of the shared key might block.
                            Thread generateThread = new Thread() {
                                @Override
                                public void run() {
                                    generateSharedSecret();
                                }
                            };
                            generateThread.setDaemon(true);
                            generateThread.start();
                        } else {
                            sharedSecret = new SecretKeySpec(
                                    stringToBytes(input.getValue()),
                                    MAC_ALGORITHM);
                            sharedSecretInitialized();
                        }
                        return null;
                    }
                }, handleDatabaseError);
    }

    private void sharedSecretInitialized() {
        // Now that the shared secret has been initialized, we are ready for
        // operation and we can send the corresponding event.
        applicationEventPublisher
                .publishEvent(new InterNodeCommunicationAuthenticationServiceInitializedEvent(
                        this));
    }

    private void startInitializeSharedSecret() {
        // We use an atomic boolean so that initializeSharedSecret is only
        // called once. It might be called again when it fails, but it will
        // never run in parallel.
        if (initializeSharedSecretStarted.compareAndSet(false, true)) {
            initializeSharedSecret();
        }
    }

}
