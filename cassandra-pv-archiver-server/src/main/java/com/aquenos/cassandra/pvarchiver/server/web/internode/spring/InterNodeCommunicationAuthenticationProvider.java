/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.internode.spring;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationAuthenticationService;
import com.google.common.collect.ImmutableList;

/**
 * <p>
 * Authentication provider for the {@link InterNodeCommunicationWebApplication}.
 * </p>
 * 
 * <p>
 * This authentication provider uses the
 * {@link InterNodeCommunicationAuthenticationService} for validating the
 * credentials supplied with a request. This way, it can validate that the
 * request was sent by another server belonging to the same cluster because only
 * such a server should have access to the shared secret that is needed to
 * generate valid credentials.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class InterNodeCommunicationAuthenticationProvider implements
        AuthenticationProvider {

    private InterNodeCommunicationAuthenticationService authenticationService;

    /**
     * Sets the inter-node-communication authentication service used by this
     * authentication provider. The authentication service is used for
     * validating the username and password passed to this provider's
     * {@link #authenticate(Authentication)} method. Typically, this method is
     * called automatically by the Spring container.
     * 
     * @param authenticationService
     *            inter-node-communication authentication service for this
     *            server.
     */
    @Autowired
    public void setAuthenticationService(
            InterNodeCommunicationAuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @Override
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {
        if (!(authentication instanceof UsernamePasswordAuthenticationToken)) {
            return null;
        }
        UsernamePasswordAuthenticationToken token = (UsernamePasswordAuthenticationToken) authentication;
        Object principal = token.getPrincipal();
        Object credentials = token.getCredentials();
        if (principal == null || credentials == null) {
            throw new BadCredentialsException(
                    "Principal and credentials must not be null.");
        }
        String username = token.getName();
        String password = credentials.toString();
        InterNodeCommunicationAuthenticationService.Role authenticationRole;
        try {
            authenticationRole = authenticationService.authenticate(username,
                    password);
        } catch (RuntimeException e) {
            throw new AuthenticationServiceException(
                    "Authentication failed because the authentication service is not available.",
                    e);
        }
        Collection<? extends GrantedAuthority> grantedAuthorities;
        switch (authenticationRole) {
        case UNPRIVILEGED:
            grantedAuthorities = ImmutableList.of(new SimpleGrantedAuthority(
                    "ROLE_INTER_NODE_UNPRIVILEGED"));
            break;
        case PRIVILEGED:
            grantedAuthorities = ImmutableList.of(new SimpleGrantedAuthority(
                    "ROLE_INTER_NODE_UNPRIVILEGED"),
                    new SimpleGrantedAuthority("ROLE_INTER_NODE_PRIVILEGED"));
            break;
        default:
            throw new BadCredentialsException("Bad credentials.");
        }
        UsernamePasswordAuthenticationToken result = new UsernamePasswordAuthenticationToken(
                authentication.getPrincipal(), authentication.getCredentials(),
                grantedAuthorities);
        return result;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class
                .isAssignableFrom(authentication);
    }

}
