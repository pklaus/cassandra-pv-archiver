/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import com.aquenos.cassandra.pvarchiver.server.database.CassandraProviderStub;
import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAOImpl;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;

/**
 * Tests for the {@link ArchiveUserDetailsManager}.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveUserDetailsManagerTest {

    /**
     * Cassandra provider that provides access to the embedded Cassandra server.
     */
    @ClassRule
    public static CassandraProviderStub cassandraProvider = new CassandraProviderStub();

    private GenericDataStoreDAOImpl genericDataStoreDao;
    private BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
    private ArchiveUserDetailsManager userDetailsManager;

    /**
     * Creates the test suite.
     * 
     * @throws Exception
     *             if there is an error while trying to initialize the
     *             components needed for the tests.
     */
    public ArchiveUserDetailsManagerTest() throws Exception {
        genericDataStoreDao = new GenericDataStoreDAOImpl();
        genericDataStoreDao
                .setApplicationEventPublisher(new ApplicationEventPublisher() {
                    @Override
                    public void publishEvent(Object event) {
                    }

                    @Override
                    public void publishEvent(ApplicationEvent event) {
                    }
                });
        genericDataStoreDao.setCassandraProvider(cassandraProvider);
        userDetailsManager = new ArchiveUserDetailsManager();
        userDetailsManager.setGenericDataStoreDAO(genericDataStoreDao);
        userDetailsManager.afterPropertiesSet();
        genericDataStoreDao.afterSingletonsInstantiated();
    }

    /**
     * Tests adding a user, changing its password, and removing it.
     */
    @Test
    public void testCreateChangePasswordDeleteRegularUser() {
        assertNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "test")));
        try {
            userDetailsManager.loadUserByUsername("test");
            assertFalse(true);
        } catch (UsernameNotFoundException e) {
            // We expect such an exception because the user should not exist
            // yet.
        }
        HashSet<GrantedAuthority> authorities = new HashSet<GrantedAuthority>();
        authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
        authorities.add(new SimpleGrantedAuthority("ROLE_FOO"));
        userDetailsManager.createUser(new User("test", passwordEncoder
                .encode("abc"), authorities));
        // We should not be able to add the user again.
        try {
            userDetailsManager.createUser(new User("test", passwordEncoder
                    .encode("abc"), authorities));
            assertFalse(true);
        } catch (IllegalArgumentException e) {
            // We expect this exception because the user already exists.
        }
        // Verify that the user actually exists.
        UserDetails user = userDetailsManager.loadUserByUsername("test");
        assertEquals("test", user.getUsername());
        assertTrue(passwordEncoder.matches("abc", user.getPassword()));
        assertEquals(authorities, user.getAuthorities());
        // Change the user's password and verify that it has been changed.
        // We have set an authentication object in the SecurityContext first
        // because the changePassword method relies on such an object being
        // present.
        Authentication oldAuthentication = SecurityContextHolder.getContext()
                .getAuthentication();
        SecurityContextHolder.getContext().setAuthentication(
                new TestingAuthenticationToken("test", ""));
        try {
            try {
                userDetailsManager.changePassword("123", "xyz");
                assertFalse(true);
            } catch (BadCredentialsException e) {
                // We expect such an exception because the old password does not
                // match.
            }
            userDetailsManager.changePassword("abc", "xyz");
            user = userDetailsManager.loadUserByUsername("test");
            assertTrue(passwordEncoder.matches("xyz", user.getPassword()));
        } finally {
            SecurityContextHolder.getContext().setAuthentication(
                    oldAuthentication);
        }
        // We also try to modify the user using the updateUser method which does
        // not require the old password.
        userDetailsManager.updateUser(new User("test", passwordEncoder
                .encode("foobar"), Collections
                .singleton(new SimpleGrantedAuthority("ROLE_ADMIN"))));
        // Verify that the user has been updated.
        user = userDetailsManager.loadUserByUsername("test");
        assertEquals("test", user.getUsername());
        assertTrue(passwordEncoder.matches("foobar", user.getPassword()));
        assertEquals(
                Collections.singleton(new SimpleGrantedAuthority("ROLE_ADMIN")),
                user.getAuthorities());
        // Remove the user.
        userDetailsManager.deleteUser("test");
        try {
            userDetailsManager.loadUserByUsername("test");
            assertFalse(true);
        } catch (UsernameNotFoundException e) {
            // We expect such an exception because the user should not exist
            // yet.
        }
        assertNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "test")));
    }

    /**
     * Tests changing the password for the admin user when it already exists in
     * the database.
     */
    @Test
    public void testChangePasswordExistingAdminUser() {
        assertNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "admin")));
        userDetailsManager.createUser(new User("admin", passwordEncoder
                .encode("hello123"), Collections
                .singleton(new SimpleGrantedAuthority("ROLE_ADMIN"))));
        // We have set an authentication object in the SecurityContext first
        // because the changePassword method relies on such an object being
        // present.
        Authentication oldAuthentication = SecurityContextHolder.getContext()
                .getAuthentication();
        SecurityContextHolder.getContext().setAuthentication(
                new TestingAuthenticationToken("admin", ""));
        try {
            try {
                userDetailsManager.changePassword("admin", "xyz");
                assertFalse(true);
            } catch (BadCredentialsException e) {
                // We expect such an exception because the old password does not
                // match.
            }
            userDetailsManager.changePassword("hello123", "xyz");
        } finally {
            SecurityContextHolder.getContext().setAuthentication(
                    oldAuthentication);
        }
        UserDetails user = userDetailsManager.loadUserByUsername("admin");
        assertEquals("admin", user.getUsername());
        assertTrue(passwordEncoder.matches("xyz", user.getPassword()));
        assertEquals(1, user.getAuthorities().size());
        assertEquals("ROLE_ADMIN", user.getAuthorities().iterator().next()
                .getAuthority());
        // We delete the record from the database in order to avoid interfering
        // with other tests.
        FutureUtils.getUnchecked(genericDataStoreDao.deleteItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "admin"));
    }

    /**
     * Tests changing the password for the admin user when it does not exist in
     * the database yet.
     */
    @Test
    public void testChangePasswordMissingAdminUser() {
        assertNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "admin")));
        // We have set an authentication object in the SecurityContext first
        // because the changePassword method relies on such an object being
        // present.
        Authentication oldAuthentication = SecurityContextHolder.getContext()
                .getAuthentication();
        SecurityContextHolder.getContext().setAuthentication(
                new TestingAuthenticationToken("admin", ""));
        try {
            try {
                userDetailsManager.changePassword("abc", "xyz");
                assertFalse(true);
            } catch (BadCredentialsException e) {
                // We expect such an exception because the old password does not
                // match.
            }
            userDetailsManager.changePassword("admin", "12345");
        } finally {
            SecurityContextHolder.getContext().setAuthentication(
                    oldAuthentication);
        }
        assertNotNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "admin")));
        UserDetails user = userDetailsManager.loadUserByUsername("admin");
        assertEquals("admin", user.getUsername());
        assertTrue(passwordEncoder.matches("12345", user.getPassword()));
        assertEquals(1, user.getAuthorities().size());
        assertEquals("ROLE_ADMIN", user.getAuthorities().iterator().next()
                .getAuthority());
        // We delete the record from the database in order to avoid interfering
        // with other tests.
        FutureUtils.getUnchecked(genericDataStoreDao.deleteItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "admin"));
    }

    /**
     * Tests changing the password for a non-special user when it does not exist
     * in the database yet.
     */
    @Test(expected = UsernameNotFoundException.class)
    public void testChangePasswordMissingRegularUser() {
        assertNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "test")));
        // We have set an authentication object in the SecurityContext first
        // because the changePassword method relies on such an object being
        // present.
        Authentication oldAuthentication = SecurityContextHolder.getContext()
                .getAuthentication();
        SecurityContextHolder.getContext().setAuthentication(
                new TestingAuthenticationToken("test", ""));
        try {
            userDetailsManager.changePassword("xyz", "12345");
        } finally {
            SecurityContextHolder.getContext().setAuthentication(
                    oldAuthentication);
        }
    }

    /**
     * Tests loading the details for the admin user when it does not exist in
     * the database yet.
     */
    @Test
    public void testLoadMissingAdminUser() {
        assertNull(FutureUtils.getUnchecked(genericDataStoreDao.getItem(
                ArchiveUserDetailsManager.GENERIC_DATA_STORE_COMPONENT_ID,
                "admin")));
        UserDetails user = userDetailsManager.loadUserByUsername("admin");
        assertEquals("admin", user.getUsername());
        assertTrue(passwordEncoder.matches("admin", user.getPassword()));
        assertEquals(1, user.getAuthorities().size());
        assertEquals("ROLE_ADMIN", user.getAuthorities().iterator().next()
                .getAuthority());
    }

}
