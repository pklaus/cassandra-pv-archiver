/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.validation.Errors;
import org.springframework.validation.SmartValidator;
import org.springframework.validation.Validator;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import com.aquenos.cassandra.pvarchiver.common.CustomUrlCodec;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.server.archiving.AddChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.AddOrUpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveServerConfigurationXmlExport;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveServerConfigurationXmlImport;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelAlreadyExistsException;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.archiving.NoSuchChannelException;
import com.aquenos.cassandra.pvarchiver.server.archiving.PendingChannelOperationException;
import com.aquenos.cassandra.pvarchiver.server.archiving.RemoveChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.UpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.cluster.ServerStatus;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.spring.ArchiveServerApplication;
import com.aquenos.cassandra.pvarchiver.server.user.ArchiveUserDetailsManager;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.aquenos.cassandra.pvarchiver.server.util.UUIDComparator;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.AbstractAddEditChannelForm;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.AbstractAddEditChannelForm.ControlSystemOption;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.AbstractAddEditChannelForm.DecimationLevel;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.AbstractAddEditChannelForm.TimePeriod;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.AbstractAddEditChannelForm.TimePeriodUnit;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.AddChannelForm;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.ChangePasswordForm;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.ConfigurationImportExportUtils;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.EditChannelForm;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;

/**
 * Controller handling requests to the administrative user-interface. This
 * controller exposes the interface through the <code>/admin/ui</code> URI.
 * 
 * @author Sebastian Marsching
 */
@Controller
@RequestMapping("/admin/ui")
public class UiController {

    /**
     * Utility class for generating the channels overview page. This class
     * implements
     * {@link ChannelOverviewServerEntry#compareTo(ChannelOverviewServerEntry)}
     * {@link ChannelOverviewServerEntry#equals(Object)}, and
     * {ChannelOverviewServerEntry{@link #hashCode()} in a way so that two
     * objects are equal if they have the same server ID and that they are
     * ordered based on the server ID and name. We can do this because we only
     * use them in a way that guarantees that the server ID is unique (and never
     * <code>null</code>).
     * 
     * @author Sebastian Marsching
     */
    private static class ChannelOverviewServerEntry
            implements Comparable<ChannelOverviewServerEntry> {

        public String serverName;
        public UUID serverId;
        public int numberOfChannels;
        // We use this field from the view, but the compiler cannot detect this.
        @SuppressWarnings("unused")
        public boolean online;

        @Override
        public int compareTo(ChannelOverviewServerEntry other) {
            return new CompareToBuilder()
                    .append(this.serverName, other.serverName)
                    .append(this.serverId, other.serverId, UUID_COMPARATOR)
                    .toComparison();
        }

        @Override
        public int hashCode() {
            return serverId.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }
            return this.serverId
                    .equals(((ChannelOverviewServerEntry) obj).serverId);
        }

    }

    private static final UUIDComparator UUID_COMPARATOR = new UUIDComparator();

    private ArchiveConfigurationService archiveConfigurationService;
    private ArchiveUserDetailsManager archiveUserDetailsManager;
    private ArchivingService archivingService;
    private CassandraProvider cassandraProvider;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ClusterManagementService clusterManagementService;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private InterNodeCommunicationService interNodeCommunicationService;

    /**
     * Sets the archive configuration service used by this controller. The
     * archive configuration service is used to make changes to the archive
     * configuration when requested by the user. Typically, this method is
     * called automatically by the Spring container.
     * 
     * @param archiveConfigurationService
     *            archive configuration service for this server.
     */
    @Autowired
    public void setArchiveConfigurationService(
            ArchiveConfigurationService archiveConfigurationService) {
        this.archiveConfigurationService = archiveConfigurationService;
    }

    /**
     * Sets the archive user-details manager. The archive user-details manager
     * is used for changing the user's password when requested by the user.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param archiveUserDetailsManager
     *            archive user-details manager that manages the users for the
     *            archiving cluster.
     */
    @Autowired
    public void setArchiveUserDetailsManager(
            ArchiveUserDetailsManager archiveUserDetailsManager) {
        this.archiveUserDetailsManager = archiveUserDetailsManager;
    }

    /**
     * Sets the archiving service. The archiving service is used to display the
     * current archiving status. Typically, this method is called automatically
     * by the Spring container.
     * 
     * @param archivingService
     *            archiving service for this server.
     */
    @Autowired
    public void setArchivingService(ArchivingService archivingService) {
        this.archivingService = archivingService;
    }

    /**
     * Sets the Cassandra provider used by this controller. The Cassandra
     * provider is used to get information about the server's connection to the
     * database. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param cassandraProvider
     *            Cassandra provider used by the archiving server.
     */
    @Autowired
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

    /**
     * Sets the channel information cache. The cache is used for reading
     * {@link ChannelInformation} objects from the database without having to
     * query the database every time. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param channelInformationCache
     *            channel information cache for getting
     *            {@link ChannelInformation} objects.
     */
    @Autowired
    public void setChannelInformationCache(
            ChannelInformationCache channelInformationCache) {
        this.channelInformationCache = channelInformationCache;
    }

    /**
     * Sets the DAO for reading and modifying meta-data related to channels. The
     * controller uses this DAO to read information about channels if the
     * service usually used for retrieving this information is temporarily not
     * available. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param channelMetaDataDAO
     *            channel meta-data DAO to be used by this object.
     */
    @Autowired
    public void setChannelMetaDataDAO(ChannelMetaDataDAO channelMetaDataDAO) {
        this.channelMetaDataDAO = channelMetaDataDAO;
    }

    /**
     * Sets the cluster management service used by this controller. The cluster
     * management service is used to get information about the cluster status
     * and to remove servers from the cluster when requested by the user.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param clusterManagementService
     *            cluster manager used by the archiving server.
     */
    @Autowired
    public void setClusterManagementService(
            ClusterManagementService clusterManagementService) {
        this.clusterManagementService = clusterManagementService;
    }

    /**
     * Sets the control-system support registry. The registry is used to gain
     * access to the control-system supports available on this archiving server.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param controlSystemSupportRegistry
     *            control-system support registry for this archiving server.
     */
    @Autowired
    public void setControlSystemSupportRegistry(
            ControlSystemSupportRegistry controlSystemSupportRegistry) {
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
    }

    /**
     * Sets the inter-node communication service. The inter-node communication
     * service is needed to communicate with other servers in the cluster when
     * displaying status information for them. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param interNodeCommunicationService
     *            inter-node communication service.
     */
    @Autowired
    public void setInterNodeCommunicationService(
            InterNodeCommunicationService interNodeCommunicationService) {
        this.interNodeCommunicationService = interNodeCommunicationService;
    }

    /**
     * Returns the dashboard view.
     * 
     * @return dashboard view and associated model parameters.
     */
    @RequestMapping("/")
    public ModelAndView dashboard() {
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("serverStatus", clusterManagementService.getThisServer());
        int channelsTotal = 0;
        int channelsDisconnected = 0;
        int channelsError = 0;
        try {
            for (ChannelStatus channelStatus : archivingService
                    .getChannelStatusForAllChannels()) {
                ++channelsTotal;
                switch (channelStatus.getState()) {
                case DISCONNECTED:
                    ++channelsDisconnected;
                    break;
                case ERROR:
                    ++channelsError;
                    break;
                default:
                    break;
                }
            }
        } catch (IllegalStateException e) {
            // When the archiving server has not been initialized yet, the
            // getChannelStatusForAllChannels() method throws an
            // IllegalStateException. We simply catch that exception and use
            // zero for all numbers concerning the channels.
        }
        modelMap.put("serverChannelsTotal", channelsTotal);
        modelMap.put("serverChannelsDisconnected", channelsDisconnected);
        modelMap.put("serverChannelsError", channelsError);
        modelMap.put("serverSamplesDropped",
                archivingService.getNumberOfSamplesDropped());
        modelMap.put("serverSamplesWritten",
                archivingService.getNumberOfSamplesWritten());
        try {
            modelMap.put("cassandraClusterName", cassandraProvider.getCluster()
                    .getMetadata().getClusterName());
            modelMap.put("cassandraKeyspaceName",
                    cassandraProvider.getSession().getLoggedKeyspace());

        } catch (RuntimeException e) {
            modelMap.put("cassandraError", e);
        }
        try {
            modelMap.put("clusterServers",
                    clusterManagementService.getServers());
        } catch (RuntimeException e) {
            // We ignore exceptions because we cannot get the cluster status if
            // this server is not online.
        }

        return new ModelAndView("dashboard", modelMap);
    }

    /**
     * <p>
     * Configures the web data-binder used for this controller. The data binder
     * is used by Spring when binding request parameters to bean properties.
     * This method replaces the validators of the binder with its own
     * implementation that effectively wraps each validator, running some extra
     * actions before and after the validation.
     * </p>
     * 
     * <p>
     * If this custom validator detects a target bean of a certain type, it will
     * run some preparations before the regular validator is run. This way, the
     * controller can deal with requests that do not fulfill all the constraints
     * of the target bean but can easily and automatically be adjusted. After
     * running the original validator, it will perform some extra validation
     * that must pass in addition to the validation performed by the standard
     * validators.
     * </p>
     * 
     * @param binder
     *            data binder that is augmented with the custom validator
     *            implementation.
     */
    @InitBinder
    public void initBinder(WebDataBinder binder) {
        ArrayList<Validator> validators = new ArrayList<Validator>(
                binder.getValidators());
        binder.replaceValidators(Lists
                .transform(validators, new Function<Validator, Validator>() {
                    @Override
                    public Validator apply(final Validator input) {
                        if (input == null) {
                            return null;
                        } else if (input instanceof SmartValidator) {
                            return new SmartValidator() {
                                @Override
                                public boolean supports(Class<?> clazz) {
                                    return input.supports(clazz);
                                }

                                @Override
                                public void validate(Object target,
                                        Errors errors) {
                                    beforeValidate(target, errors);
                                    input.validate(target, errors);
                                    afterValidate(target, errors);
                                }

                                @Override
                                public void validate(Object target,
                                        Errors errors,
                                        Object... validationHints) {
                                    beforeValidate(target, errors);
                                    ((SmartValidator) input).validate(target,
                                            errors, validationHints);
                                    afterValidate(target, errors);
                                }
                            };
                        } else {
                            return new Validator() {
                                @Override
                                public boolean supports(Class<?> clazz) {
                                    return input.supports(clazz);
                                }

                                @Override
                                public void validate(Object target,
                                        Errors errors) {
                                    beforeValidate(target, errors);
                                    input.validate(target, errors);
                                    afterValidate(target, errors);
                                }
                            };
                        }
                    }
                }).toArray(new Validator[validators.size()]));
    }

    /**
     * Returns the about view.
     * 
     * @return about view and associated model parameters.
     */
    @RequestMapping(value = "/about", method = RequestMethod.GET)
    public ModelAndView about() {
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        String implementationVersion = ArchiveServerApplication.class
                .getPackage().getImplementationVersion();
        if (implementationVersion != null
                && !implementationVersion.trim().isEmpty()) {
            modelMap.put("productVersion", implementationVersion.trim());
        }
        return new ModelAndView("about", modelMap);
    }

    /**
     * Returns the change-password view with an empty form.
     * 
     * @return change-password view with empty form.
     */
    @RequestMapping(value = "/account/change-password", method = RequestMethod.GET)
    @Secured("ROLE_AUTHENTICATED_USER")
    public ModelAndView accountChangePasswordGet() {
        ChangePasswordForm form = new ChangePasswordForm();
        return new ModelAndView("account/change-password", "form", form);
    }

    /**
     * Processes the change-password form and returns the change-password view.
     * If the form data has no errors, an attempt to change the password is
     * made. When this attempt is successful, this is indicated by a flag in the
     * model. Otherwise, the <code>formBindingResult</code> is populated with a
     * message that indicates the origin of the problem.
     * 
     * @param form
     *            change-password form.
     * @param formBindingResult
     *            form-binding result for the change-password form. This binding
     *            result is already prepopulated with the results of the
     *            annotation-based validation of the form. It is augmented with
     *            the results of the additional checks performed by this method.
     * @return change-password and the associated model.
     */
    @RequestMapping(value = "/account/change-password", method = RequestMethod.POST)
    @Secured("ROLE_AUTHENTICATED_USER")
    public ModelAndView accountChangePasswordPost(
            @ModelAttribute("form") @Valid ChangePasswordForm form,
            BindingResult formBindingResult) {
        // If there is any error that can be detected by the checks run based on
        // the annotation, we return immediately.
        if (formBindingResult.hasErrors()) {
            return new ModelAndView("account/change-password");
        }
        // Now we have to check that the two values for the new password
        // actually match. We cannot do this test with an annotation.
        if (!form.getNewPassword().equals(form.getNewPasswordRepeated())) {
            formBindingResult.rejectValue("newPasswordRepeated",
                    "MismatchError",
                    "The repeated password does not match the first password.");
            return new ModelAndView("account/change-password");
        }
        try {
            archiveUserDetailsManager.changePassword(form.getOldPassword(),
                    form.getNewPassword());
        } catch (BadCredentialsException e) {
            formBindingResult.rejectValue("oldPassword", "BadCredentialsError",
                    "The specified password is incorrect.");
            return new ModelAndView("account/change-password");
        } catch (UsernameNotFoundException e) {
            formBindingResult.reject("UsernameNotFoundError",
                    "The user does not exist in the database. Maybe it has been removed since signing in?");
            return new ModelAndView("account/change-password");
        } catch (Exception e) {
            formBindingResult.reject("DatabaseAccessError",
                    "There was an error while trying to change the password. Please try again later.");
            return new ModelAndView("account/change-password");
        }
        // The password change has been successful, so we reset all of the form
        // fields.
        form.setOldPassword("");
        form.setNewPassword("");
        form.setNewPasswordRepeated("");
        return new ModelAndView("account/change-password", "passwordChanged",
                true);
    }

    /**
     * Returns the sign-in view. This view presents a form for signing in. The
     * actual sign-in process is not handled by this method but handled by a
     * filter. If the user is already logged in (the authentication is not
     * anonymous), this method redirects to the dashboard.
     * 
     * @param authentication
     *            authentication object representing the current authentication.
     * @return sign-in view or redirect to the dashboard, depending on the
     *         current authentication state.
     */
    @RequestMapping(value = "/account/sign-in", method = RequestMethod.GET)
    public ModelAndView accountSignIn(Authentication authentication) {
        if (authentication == null
                || authentication instanceof AnonymousAuthenticationToken) {
            return new ModelAndView("account/sign-in");
        } else {
            return new ModelAndView("redirect:/admin/ui/");
        }
    }

    /**
     * Returns the sign-out view. This view presents a form signing out. The
     * actual sign-out process is not handled by this method but handled by a
     * filter. If the user is already logged out (the authentication is
     * <code>null</code> or anonymous), this method redirects to the sign-in
     * page.
     * 
     * @param authentication
     *            authentication object representing the current authentication.
     * @return sign-out view or redirect to the sign-in page, depending on the
     *         current authentication state.
     */
    @RequestMapping(value = "/account/sign-out", method = RequestMethod.GET)
    public ModelAndView accountSignOut(Authentication authentication) {
        if (authentication == null
                || authentication instanceof AnonymousAuthenticationToken) {
            return new ModelAndView("redirect:/admin/ui/account/sign-in");
        } else {
            return new ModelAndView("account/sign-out");
        }
    }

    /**
     * Returns the channels-overview view. The channels overview presents all
     * servers with information about their channels.
     * 
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            details view cannot be gathered successfully.
     * @return channels-overview view and associated model parameters.
     */
    @RequestMapping(value = "/channels", method = RequestMethod.GET)
    public ModelAndView channels(HttpServletResponse response) {
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        boolean serverInformationAvailable = true;
        int totalNumberOfChannels = 0;
        try {
            HashMap<UUID, ChannelOverviewServerEntry> serverMap = new HashMap<UUID, ChannelOverviewServerEntry>();
            Map<UUID, SortedMap<String, ChannelInformation>> channels = channelInformationCache
                    .getChannelsSortedByServer();
            for (Map.Entry<UUID, SortedMap<String, ChannelInformation>> channelEntry : channels
                    .entrySet()) {
                ChannelOverviewServerEntry serverEntry = new ChannelOverviewServerEntry();
                serverEntry.serverId = channelEntry.getKey();
                serverEntry.serverName = "unknown";
                serverEntry.online = false;
                serverEntry.numberOfChannels = channelEntry.getValue().size();
                totalNumberOfChannels += serverEntry.numberOfChannels;
                serverMap.put(serverEntry.serverId, serverEntry);
            }
            for (ServerStatus serverStatus : clusterManagementService
                    .getServers()) {
                ChannelOverviewServerEntry serverEntry = serverMap
                        .get(serverStatus.getServerId());
                if (serverEntry == null) {
                    serverEntry = new ChannelOverviewServerEntry();
                    serverEntry.serverId = serverStatus.getServerId();
                    serverEntry.numberOfChannels = 0;
                    serverMap.put(serverEntry.serverId, serverEntry);
                }
                serverEntry.serverName = serverStatus.getServerName();
                serverEntry.online = serverStatus.isOnline();
            }
            TreeSet<ChannelOverviewServerEntry> serverOverviewSet = new TreeSet<UiController.ChannelOverviewServerEntry>(
                    serverMap.values());
            modelMap.put("servers", serverOverviewSet);
        } catch (IllegalStateException e) {
            serverInformationAvailable = false;
        }
        if (!serverInformationAvailable) {
            // If we could not get the information because the service is
            // currently not available, we indicate this by sending error code
            // 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        modelMap.put("serverInformationAvailable", serverInformationAvailable);
        modelMap.put("totalNumberOfChannels", totalNumberOfChannels);
        return new ModelAndView("channels/overview", modelMap);
    }

    /**
     * Returns the all channels view. This view presents all channels,
     * regardless of the server they belong to.
     * 
     * @return all channels view and associated model parameters.
     */
    @RequestMapping(value = "/channels/all", method = RequestMethod.GET)
    public ModelAndView channelsAll() {
        // The model data is retrieved via AJAX, so we only need the view.
        return new ModelAndView("channels/all");
    }

    /**
     * Returns the channels-by-server view. This view presents all the channels
     * that belong to a server with their status.
     * 
     * @param serverId
     *            ID of the server for which the channels are to be displayed.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            details view cannot be gathered successfully.
     * @return channels-by-server view and associated model parameters.
     */
    @RequestMapping(value = "/channels/by-server/{serverId}", method = RequestMethod.GET)
    public ModelAndView channelsByServer(@PathVariable UUID serverId,
            HttpServletResponse response) {
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        String serverName = serverStatus != null ? serverStatus.getServerName()
                : "unknown";
        boolean serverExists;
        if (serverStatus == null) {
            serverName = "unknown";
            // If we cannot get the server status. Typically, this will only
            // happen when a server is not registered in the cluster any longer.
            // However, there might still be channels registered to this server,
            // so we check the database to be sure before returning a 404 error.
            try {
                serverExists = !Iterables.isEmpty(FutureUtils.getUnchecked(
                        channelMetaDataDAO.getChannelsByServer(serverId)));
            } catch (RuntimeException e) {
                // If the database is not available, we send an appropriate
                // error code.
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                return null;
            }
        } else {
            serverName = serverStatus.getServerName();
            serverExists = true;
        }
        if (!serverExists) {
            // If the server does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
        modelMap.put("serverExists", serverExists);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        return new ModelAndView("channels/by-server", modelMap);
    }

    /**
     * Returns an XML export of the channel configuration for the specified
     * server. If the channel configuration cannot be loaded, this method
     * redirects to the channels-by-server view.
     * 
     * @param serverId
     *            ID of the server for which the channel configuration shall be
     *            exported.
     * @param response
     *            HTTP servlet response. On success, this method writes the XML
     *            document directly to the response.
     * @return <code>null</code> on success, redirect to channel-details view on
     *         error.
     */
    @RequestMapping(value = "/channels/by-server/{serverId}/export", method = RequestMethod.GET)
    public ModelAndView channelsByServerExport(@PathVariable UUID serverId,
            HttpServletResponse response) {
        List<ChannelConfiguration> channelConfigurationList = ConfigurationImportExportUtils
                .getChannelConfigurationForExport(serverId, archivingService,
                        channelMetaDataDAO, clusterManagementService,
                        interNodeCommunicationService);
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        String serverName = serverStatus != null ? serverStatus.getServerName()
                : "unknown";
        // A server is considered existent if its ID is known in the cluster or
        // if there is at least one channel for that server.
        if (channelConfigurationList != null && (serverStatus != null
                || !channelConfigurationList.isEmpty())) {
            ArchiveServerConfigurationXmlExport xmlExport = new ArchiveServerConfigurationXmlExport(
                    channelConfigurationList);
            response.setContentType("application/xml;charset=UTF-8");
            response.setCharacterEncoding("UTF-8");
            // We only allow characters in the filename that are safe on
            // virtually all platforms (and are also safe for use in an HTTP
            // header).
            String filename = (new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")
                    .format(new Date())) + "_"
                    + serverName.replaceAll("[^A-Za-z0-9_\\-.]", "") + ".xml";
            response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    "inline; filename=\"" + filename + "\"");
            try {
                xmlExport.serialize(response.getOutputStream(), "UTF-8");
            } catch (IOException e) {
                throw new RuntimeException(
                        "Unexpected I/O exception when trying to get the output stream for the HTTP response: "
                                + e.getMessage(),
                        e);
            }
            // We already handled the response, so Spring should not take care
            // of it.
            return null;
        } else {
            // If there is any error (server does not exist, database not
            // available, etc.), we redirect to the channel list view. The
            // channel list view basically does the same that we do here, so it
            // will display the appropriate error.
            return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                    + serverId.toString() + "/");
        }
    }

    /**
     * Returns the channels-import view.
     * 
     * @param serverId
     *            ID of the server for which channels shall be imported. This
     *            method does not check whether the server actually exists
     *            because there might be reasons to import channels for a server
     *            that does not exist yet.
     * @return channels-import view and associated model paramters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/import", method = RequestMethod.GET)
    public ModelAndView channelsByServerImportGet(@PathVariable UUID serverId) {
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        String serverName = (serverStatus != null)
                ? serverStatus.getServerName() : "unknown";
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("addChannels", true);
        modelMap.put("addOrUpdateFailed", null);
        modelMap.put("addOrUpdateSucceeded", null);
        modelMap.put("fileError", null);
        modelMap.put("globalError", null);
        modelMap.put("removeChannels", false);
        modelMap.put("removeFailed", null);
        modelMap.put("removeSucceded", null);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("showForm", true);
        modelMap.put("showResult", false);
        modelMap.put("updateChannels", true);
        return new ModelAndView("channels/import", modelMap);
    }

    /**
     * Processes a form submitted from the channels-import view. After
     * successfully processing the form data and importing the channels, the
     * channels-import view is returned with the results of the import
     * operation. If there is a problem with the form data, the channels-import
     * view is shown again with the form and appropriate error messages.
     * 
     * @param serverId
     *            ID of the server for which channels shall be imported. This
     *            method does not check whether the server actually exists
     *            because there might be reasons to import channels for a server
     *            that does not exist yet.
     * @param file
     *            uploaded configuration file. This file is processed by
     *            {@link ArchiveServerConfigurationXmlImport}.
     * @param addChannels
     *            tells whether channels should be added to the server
     *            configuration. If <code>true</code> channels that exist in the
     *            configuration file but not on the server are added. If
     *            <code>false</code> channels that exist in the configuration
     *            file but not on the server are not added.
     * @param updateChannels
     *            tells whether channels on the server should be updated. If
     *            <code>true</code>, channels that exist in the configuration
     *            file and on the server are updated with the configuration
     *            specified in the configuration file. If <code>false</code>,
     *            channels in the configuration file that also exist on the
     *            server are not touched.
     * @param removeChannels
     *            tells whether channels that only exist on the server should be
     *            removed. If <code>true</code>, channels that exist on the
     *            server but not in the configuration file are removed. If
     *            <code>false</code>, channels that exist on the server but not
     *            in the configuration file are simply ignored.
     * @return channels-import view and associated model paramters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/import", method = RequestMethod.POST)
    public ModelAndView channelsByServerImportPost(@PathVariable UUID serverId,
            @RequestParam(required = false) MultipartFile file,
            @RequestParam(defaultValue = "false") boolean addChannels,
            @RequestParam(defaultValue = "false") boolean updateChannels,
            @RequestParam(defaultValue = "false") boolean removeChannels) {
        DefaultMessageSourceResolvable fileError = null;
        ArchiveServerConfigurationXmlImport xmlImport;
        if (file == null || file.getSize() == 0) {
            fileError = new DefaultMessageSourceResolvable("NotNull");
            xmlImport = null;
        } else {
            InputStream inputStream;
            try {
                inputStream = file.getInputStream();
            } catch (IOException e) {
                // An IOException here is very unlikely so that we simply throw
                // it
                // and let the default error handler handle it.
                throw new RuntimeException(
                        "Reading the uploaded file failed: " + e.getMessage(),
                        e);
            }
            try {
                xmlImport = new ArchiveServerConfigurationXmlImport(
                        inputStream);
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    Throwable cause = e.getCause();
                    if (cause.getMessage() != null) {
                        exceptionMessage = cause.getMessage();
                    } else {
                        exceptionMessage = e.getClass().getName();
                    }
                }
                fileError = new DefaultMessageSourceResolvable(
                        new String[] { "ParsingError" },
                        new Object[] { exceptionMessage });
                xmlImport = null;
            }
        }
        DefaultMessageSourceResolvable globalError = null;
        TreeSet<String> addOrUpdateSucceeded = null;
        TreeSet<String> removeSucceeded = null;
        TreeMap<String, String> addOrUpdateFailed = null;
        TreeMap<String, String> removeFailed = null;
        List<ArchiveConfigurationCommand> commands = null;
        if (xmlImport != null) {
            try {
                commands = ConfigurationImportExportUtils
                        .generateConfigurationCommands(xmlImport, addChannels,
                                removeChannels, updateChannels, serverId,
                                channelInformationCache, channelMetaDataDAO);
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = e.getClass().getName();
                }
                globalError = new DefaultMessageSourceResolvable(
                        new String[] { "DatabaseAccessError" },
                        new Object[] { exceptionMessage });
            }
        }
        // We only continue if there was no error while creating the list of
        // commands.
        List<ArchiveConfigurationCommandResult> results = null;
        if (commands != null) {
            try {
                results = FutureUtils.getUnchecked(archiveConfigurationService
                        .runConfigurationCommands(commands));
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = e.getClass().getName();
                }
                globalError = new DefaultMessageSourceResolvable(
                        new String[] { "DatabaseAccessError" },
                        new Object[] { exceptionMessage });
            }
        }
        if (results != null) {
            addOrUpdateSucceeded = new TreeSet<String>();
            removeSucceeded = new TreeSet<String>();
            addOrUpdateFailed = new TreeMap<String, String>();
            removeFailed = new TreeMap<String, String>();
            for (ArchiveConfigurationCommandResult result : results) {
                switch (result.getCommand().getCommandType()) {
                case ADD_CHANNEL:
                    if (result.isSuccess()) {
                        addOrUpdateSucceeded
                                .add(((AddChannelCommand) result.getCommand())
                                        .getChannelName());
                    } else {
                        addOrUpdateFailed.put(
                                ((AddChannelCommand) result.getCommand())
                                        .getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                case ADD_OR_UPDATE_CHANNEL:
                    if (result.isSuccess()) {
                        addOrUpdateSucceeded
                                .add(((AddOrUpdateChannelCommand) result
                                        .getCommand()).getChannelName());
                    } else {
                        addOrUpdateFailed.put(
                                ((AddOrUpdateChannelCommand) result
                                        .getCommand()).getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                case REMOVE_CHANNEL:
                    if (result.isSuccess()) {
                        removeSucceeded.add(
                                ((RemoveChannelCommand) result.getCommand())
                                        .getChannelName());
                    } else {
                        removeFailed.put(
                                ((RemoveChannelCommand) result.getCommand())
                                        .getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                case UPDATE_CHANNEL:
                    if (result.isSuccess()) {
                        addOrUpdateSucceeded.add(
                                ((UpdateChannelCommand) result.getCommand())
                                        .getChannelName());
                    } else {
                        addOrUpdateFailed.put(
                                ((UpdateChannelCommand) result.getCommand())
                                        .getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                default:
                    // We should never see any other commands because we can
                    // only get results for commands that we have sent.
                    break;
                }
            }
        }
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        String serverName = (serverStatus != null)
                ? serverStatus.getServerName() : "unknown";
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("addChannels", addChannels);
        modelMap.put("addOrUpdateFailed", addOrUpdateFailed);
        modelMap.put("addOrUpdateSucceeded", addOrUpdateSucceeded);
        modelMap.put("fileError", fileError);
        modelMap.put("globalError", globalError);
        modelMap.put("removeChannels", removeChannels);
        modelMap.put("removeFailed", removeFailed);
        modelMap.put("removeSucceeded", removeSucceeded);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("showForm", (globalError != null) || (fileError != null));
        modelMap.put("showResult",
                (globalError == null) && (fileError == null));
        modelMap.put("updateChannels", updateChannels);
        return new ModelAndView("channels/import", modelMap);
    }

    /**
     * Returns the channel-add view.
     * 
     * @return channel-add view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/add", method = RequestMethod.GET)
    public ModelAndView channelAddGet() {
        return channelAddPrepareForm(null, null, "all");
    }

    /**
     * Processes a form submitted from the channel-add view. In case of error,
     * the channel-add view is shown again. In case of success, the user is
     * redirected to the channel-details view.
     * 
     * @param form
     *            bean storing the form data.
     * @param formBindingResult
     *            validation result of the <code>form</code> bean.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-add view
     *         on error. In case of a severe error that results in an error page
     *         to be sent directly, <code>null</code> is returned.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/add", method = RequestMethod.POST)
    public ModelAndView channelAddPost(
            @ModelAttribute("form") @Valid AddChannelForm form,
            BindingResult formBindingResult, HttpServletResponse response) {
        return channelAddProcessForm(null, form, formBindingResult, response,
                "all");
    }

    /**
     * Returns the channel-details view. This view presents the status and the
     * configuration of a specific channel. This method is used when a channel
     * is accessed from the "all channels" list.
     * 
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            details view cannot be gathered successfully.
     * @return channel-details view and associated model parameters.
     * @see #channelByServerDetails(UUID, String, HttpServletResponse)
     */
    @RequestMapping(value = "/channels/all/by-name/{channelName}", method = RequestMethod.GET)
    public ModelAndView channelDetails(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelDetails(null, channelName, response, "all");
    }

    /**
     * Returns the channel-edit view.
     * 
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the edit
     *            view cannot be gathered successfully.
     * @return channel-edit view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/edit", method = RequestMethod.GET)
    public ModelAndView channelEditGet(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelEditPrepareForm(channelName, null, response, "all");
    }

    /**
     * Processes a form submitted from the channel-edit view. In case of error,
     * the channel-edit view is shown again. In case of success, the user is
     * redirected to the channel-details view.
     * 
     * @param channelName
     *            name of the channel.
     * @param form
     *            bean storing the form data.
     * @param formBindingResult
     *            validation result of the <code>form</code> bean.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-edit view
     *         on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/edit", method = RequestMethod.POST)
    public ModelAndView channelEditPost(@PathVariable String channelName,
            @ModelAttribute("form") @Valid EditChannelForm form,
            BindingResult formBindingResult, HttpServletResponse response) {
        return channelEditProcessForm(channelName, null, form,
                formBindingResult, response, "all");
    }

    /**
     * Returns the channel-move view.
     * 
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            channel-move view cannot be gathered successfully.
     * @return channel-move view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/move", method = RequestMethod.GET)
    public ModelAndView channelMoveGet(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelMovePrepareForm(channelName, null, response, "all");
    }

    /**
     * Processes a form submitted from the channel-move view. In case of error,
     * the channel-move view is shown again. In case of success, the user is
     * redirected to the channel-details view.
     * 
     * @param channelName
     *            old name of the channel.
     * @param oldServerId
     *            ID of the server from which the channel shall be moved.
     * @param newServerId
     *            ID of the server to which the channel shall be moved.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-move view
     *         on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/move", method = RequestMethod.POST)
    public ModelAndView channelMovePost(@PathVariable String channelName,
            @RequestParam UUID oldServerId,
            @RequestParam(required = false) UUID newServerId,
            HttpServletResponse response) {
        return channelMoveProcessForm(channelName, null, oldServerId,
                newServerId, response, "all");
    }

    /**
     * Reinitializes a channel. In case of error, the reinitialize-channel view
     * is shown. On success, the user is redirected to the channel-details view.
     * 
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and
     *         channel-reinitialize view on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/reinitialize", method = RequestMethod.POST)
    public ModelAndView channelReinitialize(@PathVariable String channelName,
            HttpServletResponse response) {
        // The channel name is encoded, so we have to decoded it first.
        channelName = CustomUrlCodec.decode(channelName);
        boolean channelExists;
        UUID serverId;
        try {
            ChannelInformation channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            if (channelInformation == null) {
                channelExists = false;
                serverId = null;
            } else {
                channelExists = true;
                serverId = channelInformation.getServerId();
            }
        } catch (RuntimeException e) {
            channelExists = true;
            serverId = null;
        }
        boolean refreshSuccessful;
        if (serverId == null) {
            refreshSuccessful = false;
        } else {
            // We only try to refresh the channel if the target server is
            // online. The operation would simply succeed if the target server
            // was not online, but reporting success to the user might give the
            // wrong idea.
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus == null || !serverStatus.isOnline()) {
                refreshSuccessful = false;
            } else {
                try {
                    FutureUtils.getUnchecked(archiveConfigurationService
                            .refreshChannel(serverId, channelName));
                    refreshSuccessful = true;
                } catch (RuntimeException e) {
                    refreshSuccessful = false;
                }
            }
        }
        if (refreshSuccessful) {
            return new ModelAndView("redirect:/admin/ui/channels/all/by-name/"
                    + CustomUrlCodec.encode(channelName)
                    + "/?message.refreshed");
        } else {
            // If we got here and the channel exists, there must have been some
            // internal problem. If the channel does not exist, a 404 response
            // seems appropriate.
            if (channelExists) {
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
            HashMap<String, Object> modelMap = new HashMap<String, Object>();
            modelMap.put("channelExists", channelExists);
            modelMap.put("viewMode", "all");
            return new ModelAndView("channels/reinitialize", modelMap);
        }
    }

    /**
     * Returns the channel-remove view.
     * 
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            channel-remove view cannot be gathered successfully.
     * @return channel-remove view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/remove", method = RequestMethod.GET)
    public ModelAndView channelRemoveGet(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelRemovePrepareForm(channelName, null, response, "all");
    }

    /**
     * Processes a form submitted from the channel-remove view. In case of
     * error, the channel-remove view is shown again. In case of success, the
     * user is redirected to the channel-details view.
     * 
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-remove
     *         view on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/remove", method = RequestMethod.POST)
    public ModelAndView channelRemovePost(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelRemoveProcessForm(channelName, null, response, "all");
    }

    /**
     * Returns the channel-rename view.
     * 
     * @param channelName
     *            old name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            channel-rename view cannot be gathered successfully.
     * @return channel-rename view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{channelName}/rename", method = RequestMethod.GET)
    public ModelAndView channelRenameGet(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelRenamePrepareForm(channelName, null, response, "all");
    }

    /**
     * Processes a form submitted from the channel-rename view. In case of
     * error, the channel-rename view is shown again. In case of success, the
     * user is redirected to the channel-details view.
     * 
     * @param oldChannelName
     *            old name of the channel.
     * @param newChannelName
     *            new name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-rename
     *         view on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/all/by-name/{oldChannelName}/rename", method = RequestMethod.POST)
    public ModelAndView channelRenamePost(@PathVariable String oldChannelName,
            @RequestParam(required = false) String newChannelName,
            HttpServletResponse response) {
        return channelRenameProcessForm(oldChannelName, null, newChannelName,
                response, "all");
    }

    /**
     * Returns the channel-add view.
     * 
     * @param serverIdFromPath
     *            server ID specified as part of the URI path. This is the
     *            server to which the user wants to add a channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return channel-add view and associated model parameters.
     */
    @RequestMapping(value = "/channels/by-server/{serverIdFromPath}/add", method = RequestMethod.GET)
    @Secured("ROLE_ADMIN")
    public ModelAndView channelByServerAddGet(
            @PathVariable UUID serverIdFromPath, HttpServletResponse response) {
        return channelAddPrepareForm(serverIdFromPath, response, "by-server");
    }

    /**
     * Processes a form submitted from the channel-add view. In case of error,
     * the channel-add view is shown again. In case of success, the user is
     * redirected to the channel-details view.
     * 
     * @param serverIdFromPath
     *            server ID specified as part of the URI path. This must match
     *            the server ID specified in the form data.
     * @param form
     *            bean storing the form data.
     * @param formBindingResult
     *            validation result of the <code>form</code> bean.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-add view
     *         on error. In case of a severe error that results in an error page
     *         to be sent directly, <code>null</code> is returned.
     */
    @RequestMapping(value = "/channels/by-server/{serverIdFromPath}/add", method = RequestMethod.POST)
    @Secured("ROLE_ADMIN")
    public ModelAndView channelByServerAddPost(
            @PathVariable UUID serverIdFromPath,
            @ModelAttribute("form") @Valid AddChannelForm form,
            BindingResult formBindingResult, HttpServletResponse response) {
        return channelAddProcessForm(serverIdFromPath, form, formBindingResult,
                response, "by-server");
    }

    /**
     * Returns the channel-details view. This view presents the status and the
     * configuration of a specific channel. This method is used when a channel
     * is accessed from the "channels by server" list. If the specified server
     * does not own the channel but the channel exists, the user is redirected
     * to the corresponding view of the server that actually owns the channel.
     * 
     * @param serverId
     *            ID of the server that is supposed to own the channel.
     * @param channelName
     *            name of the channel
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            details view cannot be gathered successfully.
     * @return channel-details view and associated model parameters.
     * @see #channelDetails(String, HttpServletResponse)
     */
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}", method = RequestMethod.GET)
    public ModelAndView channelByServerDetails(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        return channelDetails(serverId, channelName, response, "by-server");
    }

    /**
     * Returns the channel-edit view. If the specified server does not own the
     * channel but the channel exists, the user is redirected to the
     * corresponding view of the server that actually owns the channel.
     * 
     * @param serverId
     *            ID of the server that is supposed to own the channel.
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the edit
     *            view cannot be gathered successfully.
     * @return channel-edit view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/edit", method = RequestMethod.GET)
    public ModelAndView channelByServerEditGet(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        return channelEditPrepareForm(channelName, serverId, response,
                "by-server");
    }

    /**
     * Processes a form submitted from the channel-edit view. In case of error,
     * the channel-edit view is shown again. In case of success, the user is
     * redirected to the channel-details view.
     * 
     * @param serverId
     *            server ID specified as part of the URI path. However, this
     *            information is not really used because a redirect for a POST
     *            request is problematic.
     * @param channelName
     *            name of the channel.
     * @param form
     *            bean storing the form data.
     * @param formBindingResult
     *            validation result of the <code>form</code> bean.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-edit view
     *         on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/edit", method = RequestMethod.POST)
    public ModelAndView channelByServerEditPost(@PathVariable UUID serverId,
            @PathVariable String channelName,
            @ModelAttribute("form") @Valid EditChannelForm form,
            BindingResult formBindingResult, HttpServletResponse response) {
        return channelEditProcessForm(channelName, serverId, form,
                formBindingResult, response, "by-server");
    }

    /**
     * Returns the channel-move view. If the specified server does not own the
     * channel but the channel exists, the user is redirected to the
     * corresponding view of the server that actually owns the channel.
     * 
     * @param serverId
     *            ID of the server that is supposed to own the channel.
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            channel-move view cannot be gathered successfully.
     * @return channel-move view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/move", method = RequestMethod.GET)
    public ModelAndView channelByServerMoveGet(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        return channelMovePrepareForm(channelName, serverId, response,
                "by-server");
    }

    /**
     * Processes a form submitted from the channel-move view. In case of error,
     * the channel-move view is shown again. In case of success, the user is
     * redirected to the channel-details view.
     * 
     * @param serverId
     *            server ID specified as part of the URI path. However, this
     *            information is not really used because a redirect for a POST
     *            request is problematic.
     * @param channelName
     *            old name of the channel.
     * @param oldServerId
     *            ID of the server from which the channel shall be moved.
     * @param newServerId
     *            ID of the server to which the channel shall be moved.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-move view
     *         on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/move", method = RequestMethod.POST)
    public ModelAndView channelByServerMovePost(@PathVariable UUID serverId,
            @PathVariable String channelName, @RequestParam UUID oldServerId,
            @RequestParam(required = false) UUID newServerId,
            HttpServletResponse response) {
        return channelMoveProcessForm(channelName, serverId, oldServerId,
                newServerId, response, "by-server");
    }

    /**
     * Reinitializes a channel. In case of error, the reinitialize-channel view
     * is shown. On success, the user is redirected to the channel-details view.
     * 
     * @param serverId
     *            ID of the server on which the channel is supposed to be
     *            reinitialized.
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and
     *         channel-reinitialize view on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/reinitialize", method = RequestMethod.POST)
    public ModelAndView channelByServerReinitialize(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        // The channel name is encoded, so we have to decoded it first.
        channelName = CustomUrlCodec.decode(channelName);
        // We do not check whether the channel exists. There are situations in
        // which it makes sense to reinitialize a channel even if it does not
        // exist (or does not seem to exist). This also means that we always
        // reinitialize it on the requested server, not on the server that owns
        // the channel.
        boolean refreshSuccessful;
        // We only try to refresh the channel if the target server is
        // online. The operation would simply succeed if the target server
        // was not online, but reporting success to the user might give the
        // wrong idea.
        String serverName;
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        if (serverStatus == null) {
            serverName = "unknown";
            refreshSuccessful = false;
        } else if (!serverStatus.isOnline()) {
            serverName = serverStatus.getServerName();
            refreshSuccessful = false;
        } else {
            serverName = serverStatus.getServerName();
            try {
                FutureUtils.getUnchecked(archiveConfigurationService
                        .refreshChannel(serverId, channelName));
                refreshSuccessful = true;
            } catch (RuntimeException e) {
                refreshSuccessful = false;
            }
        }
        if (refreshSuccessful) {
            return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                    + serverId.toString() + "/by-name/"
                    + CustomUrlCodec.encode(channelName)
                    + "/?message.refreshed");
        } else {
            // If we got here, there was a problem with refreshing the channel,
            // so we want to set the appropriate error code.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            HashMap<String, Object> modelMap = new HashMap<String, Object>();
            modelMap.put("channelExists", true);
            modelMap.put("serverId", serverId);
            modelMap.put("serverName", serverName);
            modelMap.put("viewMode", "by-server");
            return new ModelAndView("channels/reinitialize", modelMap);
        }
    }

    /**
     * Returns the channel-remove view. If the specified server does not own the
     * channel but the channel exists, the user is redirected to the
     * corresponding view of the server that actually owns the channel.
     * 
     * @param serverId
     *            ID of the server that is supposed to own the channel.
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            channel-remove view cannot be gathered successfully.
     * @return channel-remove view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/remove", method = RequestMethod.GET)
    public ModelAndView channelByServerRemoveGet(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        return channelRemovePrepareForm(channelName, serverId, response,
                "by-server");
    }

    /**
     * Processes a form submitted from the channel-remove view. In case of
     * error, the channel-remove view is shown again. In case of success, the
     * user is redirected to the channel-details view.
     * 
     * @param serverId
     *            server ID specified as part of the URI path. However, this
     *            information is not really used because a redirect for a POST
     *            request is problematic.
     * @param channelName
     *            name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-remove
     *         view on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/remove", method = RequestMethod.POST)
    public ModelAndView channelByServerRemovePost(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        return channelRemoveProcessForm(channelName, serverId, response,
                "by-server");
    }

    /**
     * Returns the channel-rename view. If the specified server does not own the
     * channel but the channel exists, the user is redirected to the
     * corresponding view of the server that actually owns the channel.
     * 
     * @param serverId
     *            ID of the server that is supposed to own the channel.
     * @param channelName
     *            old name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the information for the
     *            channel-rename view cannot be gathered successfully.
     * @return channel-rename view and associated model parameters.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{channelName}/rename", method = RequestMethod.GET)
    public ModelAndView channelByServerRenameGet(@PathVariable UUID serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        return channelRenamePrepareForm(channelName, serverId, response,
                "by-server");
    }

    /**
     * Processes a form submitted from the channel-rename view. In case of
     * error, the channel-rename view is shown again. In case of success, the
     * user is redirected to the channel-details view.
     * 
     * @param serverId
     *            server ID specified as part of the URI path. However, this
     *            information is not really used because a redirect for a POST
     *            request is problematic.
     * @param oldChannelName
     *            old name of the channel.
     * @param newChannelName
     *            new name of the channel.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case of an error.
     * @return redirect to channel-details view on success and channel-rename
     *         view on error.
     */
    @Secured("ROLE_ADMIN")
    @RequestMapping(value = "/channels/by-server/{serverId}/by-name/{oldChannelName}/rename", method = RequestMethod.POST)
    public ModelAndView channelByServerRenamePost(@PathVariable UUID serverId,
            @PathVariable String oldChannelName,
            @RequestParam(required = false) String newChannelName,
            HttpServletResponse response) {
        return channelRenameProcessForm(oldChannelName, serverId,
                newChannelName, response, "by-server");
    }

    /**
     * Removes the specified server from the cluster. This is a privileged
     * operation that requires administrative privileges. After removing the
     * server, this method redirects to the dashboard.
     * 
     * @param serverId
     *            unique identifier of the server to be removed.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the server cannot be removed.
     * @return redirect to the dashboard view or <code>null</code> if the
     *         response has already been sent.
     * @throws IllegalArgumentException
     *             if the server with the specified ID may not be removed.
     */
    @RequestMapping(value = "/servers/{serverId}/remove", method = RequestMethod.POST)
    @Secured("ROLE_ADMIN")
    public ModelAndView serversRemove(@PathVariable UUID serverId,
            HttpServletResponse response) {
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        if (serverStatus == null) {
            try {
                response.sendError(HttpServletResponse.SC_NOT_FOUND,
                        "The specified server does not exist.");
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error while trying to send error response: "
                                + e.getMessage(),
                        e);
            }
            return null;
        } else if (!serverStatus.isRemovable()) {
            throw new IllegalArgumentException("Server " + serverId.toString()
                    + " cannot be removed because it has not been offline for a sufficient time.");
        }
        clusterManagementService.removeServer(serverId);
        return new ModelAndView("redirect:/admin/ui/");
    }

    private void afterValidate(Object target, Errors errors) {
        if (target instanceof AbstractAddEditChannelForm) {
            afterValidate((AbstractAddEditChannelForm) target, errors);
        }
    }

    private void afterValidate(AbstractAddEditChannelForm target,
            Errors errors) {
        // In addition to the annotation-based validations, we want to run some
        // validations that can not be expressed with annotations. For the
        // decimation levels, we apply some normalizations at the same time.
        NavigableMap<Integer, Boolean> removeDecimationLevels;
        if (target instanceof EditChannelForm) {
            removeDecimationLevels = ((EditChannelForm) target)
                    .getRemoveDecimationLevels();
        } else {
            removeDecimationLevels = null;
        }
        normalizeAndValidateDecimationLevels(target, errors,
                removeDecimationLevels);
        validateOptions(target, errors);
    }

    private void beforeValidate(Object target, Errors errors) {
        if (target instanceof AbstractAddEditChannelForm) {
            beforeValidate((AbstractAddEditChannelForm) target);
        }
    }

    private void beforeValidate(AbstractAddEditChannelForm target) {
        // We want the decimation-levels map to be not null. We do not require
        // this from the user input, but it makes our life much simpler.
        if (target.getDecimationLevels() == null) {
            target.setDecimationLevels(new TreeMap<Integer, DecimationLevel>());
        }
        // We do not allow null keys. As there is no reasonable way how an entry
        // with such a key might get added to the map, we simply remove it if
        // present. This way, the rest of our validation logic can simply rely
        // on not finding null keys.
        try {
            target.getDecimationLevels().remove(null);
        } catch (NullPointerException e) {
            // If the map implementation does not support null keys, we are
            // safe.
        }
        // The same applies to the options map.
        if (target.getOptions() == null) {
            target.setOptions(new TreeMap<Integer, ControlSystemOption>());
        }
        try {
            target.getOptions().remove(null);
        } catch (NullPointerException e) {
            // Ignore
        }
        // For an EditChannelForm, we also have to process the two remove maps.
        if (target instanceof EditChannelForm) {
            EditChannelForm typedTarget = (EditChannelForm) target;
            if (typedTarget.getRemoveDecimationLevels() == null) {
                typedTarget.setRemoveDecimationLevels(
                        new TreeMap<Integer, Boolean>());
            }
            try {
                typedTarget.getRemoveDecimationLevels().remove(null);
            } catch (NullPointerException e) {
                // Ignore
            }
            if (typedTarget.getRemoveOptions() == null) {
                typedTarget.setRemoveOptions(new TreeMap<Integer, Boolean>());
            }
            try {
                typedTarget.getRemoveOptions().remove(null);
            } catch (NullPointerException e) {
                // Ignore
            }
        }
    }

    private ModelAndView channelAddPrepareForm(UUID serverIdFromRequest,
            HttpServletResponse response, String viewMode) {
        AddChannelForm form = new AddChannelForm();
        form.setDecimationLevels(
                ImmutableSortedMap.of(0, defaultRawDecimationLevel()));
        form.setEnabled(true);
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("viewMode", viewMode);
        if (serverIdFromRequest == null) {
            // If the server has not been specified in the path, we preselect
            // the on which the UI is running.
            form.setServerId(
                    clusterManagementService.getThisServer().getServerId());
        } else {
            // If the server has been specified in the path, we use that server.
            // In this case, this will just be a hidden field and the user will
            // actually not have a choice.
            form.setServerId(serverIdFromRequest);
        }
        modelMap.put("form", form);
        populateModelWithControlSystemTypes(modelMap);
        populateModelWithServerInformation(modelMap, serverIdFromRequest);
        // If the server from the request path does not exist, we want to show
        // an error message and send a 404 error.
        if (modelMap.get("serverExist") != null
                && (Boolean) modelMap.get("serverExists")) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
        return new ModelAndView("channels/add", modelMap);
    }

    private ModelAndView channelAddProcessForm(UUID serverIdFromRequest,
            AddChannelForm form, BindingResult formBindingResult,
            HttpServletResponse response, String viewMode) {
        // If there was a validation error, we again show the form to the user.
        if (formBindingResult.hasErrors()) {
            return channelAddProcessFormError(serverIdFromRequest, form,
                    formBindingResult, response, viewMode);
        }
        // We check whether the specified channel name is still available.
        try {
            if (FutureUtils.getUnchecked(channelInformationCache
                    .getChannelAsync(form.getChannelName(), false)) != null) {
                formBindingResult.rejectValue("channelName",
                        "ChannelNameUnique", "channel name already in use");
            }
        } catch (RuntimeException e) {
            // If we cannot check whether the channel exists, there is no sense
            // in trying to add the channel because this database operation will
            // most likely fail as well.
            String exceptionMessage = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName();
            formBindingResult.reject("DatabaseAccessError",
                    new Object[] { exceptionMessage },
                    "Error while accessing the database.");
        }
        // We check whether the specified control-system type exists: It is
        // possible that a certain control-system type is only supported by
        // specific servers. However, we only show control-system types
        // supported by this server to the user anyway. By limiting ourselves to
        // these servers, we can ensure that the channel can later be removed (a
        // channel cannot be removed if the control-system support is not
        // present).
        if (controlSystemSupportRegistry
                .getControlSystemSupport(form.getControlSystemType()) == null) {
            formBindingResult.rejectValue("controlSystemType",
                    "ControlSystemSupportAvailable",
                    "The specified control-system support is not available.");
        }
        // If a server ID has been specified in the path, it should match the
        // server ID from the form. If not, someone messed with the form data
        // and we simply return an error.
        if (serverIdFromRequest != null
                && !serverIdFromRequest.equals(form.getServerId())) {
            try {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "The server ID from the path and the server ID from the form data do not match.");
                return null;
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error while trying to send error response: "
                                + e.getMessage(),
                        e);
            }
        }
        // We have to check for errors again because we might have added some.
        if (formBindingResult.hasErrors()) {
            return channelAddProcessFormError(serverIdFromRequest, form,
                    formBindingResult, response, viewMode);
        }
        // If the validation was successful, we know that the data in the form
        // is well-formed and thus we can use it without any further checks.
        Map<Integer, Integer> decimationLevelToRetentionPeriod = new HashMap<Integer, Integer>();
        for (DecimationLevel decimationLevel : form.getDecimationLevels()
                .values()) {
            decimationLevelToRetentionPeriod.put(
                    decimationLevel.getDecimationPeriod().getSeconds(),
                    decimationLevel.getRetentionPeriod().getSeconds());
        }
        Map<String, String> options = new HashMap<String, String>();
        for (ControlSystemOption option : form.getOptions().values()) {
            options.put(option.getName(), option.getValue());
        }
        try {
            FutureUtils.getUnchecked(
                    archiveConfigurationService.addChannel(form.getServerId(),
                            form.getChannelName(), form.getControlSystemType(),
                            decimationLevelToRetentionPeriod.keySet(),
                            decimationLevelToRetentionPeriod, form.getEnabled(),
                            options));
        } catch (ChannelAlreadyExistsException e) {
            formBindingResult.rejectValue("channelName", "ChannelNameUnique",
                    "channel name already in use");
        } catch (PendingChannelOperationException e) {
            String exceptionMessage = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName();
            formBindingResult.reject("PendingChannelOperationError",
                    new Object[] { exceptionMessage },
                    "Another operation is pending.");
        } catch (RuntimeException e) {
            String exceptionMessage = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName();
            formBindingResult.reject("DatabaseAccessError",
                    new Object[] { exceptionMessage },
                    "Error while accessing the database.");
        }
        // If the operation failed, we added an error to the binding result.
        if (formBindingResult.hasErrors()) {
            return channelAddProcessFormError(serverIdFromRequest, form,
                    formBindingResult, response, viewMode);
        }
        // After successfully creating the channel, we redirect to its details
        // page. Depending on the view-mode, the URL looks slightly different.
        if (serverIdFromRequest == null) {
            return new ModelAndView("redirect:/admin/ui/channels/all/by-name/"
                    + CustomUrlCodec.encode(form.getChannelName())
                    + "/?message.created");
        } else {
            return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                    + serverIdFromRequest.toString() + "/by-name/"
                    + CustomUrlCodec.encode(form.getChannelName())
                    + "/?message.created");
        }
    }

    private ModelAndView channelAddProcessFormError(UUID serverIdFromRequest,
            AddChannelForm form, BindingResult formBindingResult,
            HttpServletResponse response, String viewMode) {
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("viewMode", viewMode);
        populateModelWithControlSystemTypes(modelMap);
        populateModelWithServerInformation(modelMap, serverIdFromRequest);
        // If the server from the request path does not exist, we want to show
        // an error message and send a 404 error.
        if (modelMap.get("serverExist") != null
                && (Boolean) modelMap.get("serverExists")) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
        return new ModelAndView("channels/add", modelMap);
    }

    private ModelAndView channelDetails(UUID serverIdFromRequest,
            String channelName, HttpServletResponse response, String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        // If the request was made for a specific server, we have to check
        // whether the specified server is actually the server that owns the
        // channel. If the request was made without specifying a server, we have
        // to find out which server the channel belongs to.
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // configuration available, there must have been a problem
            // retrieving the information.
            channelExists = true;
            channelInformation = null;
        }
        ChannelStatus channelStatus = null;
        if (channelInformation != null) {
            // The channel exists, but does it belong to the specified server
            // (if a server was specified)? If not, we want to redirect to the
            // correct server.
            if (serverIdFromRequest != null && !serverIdFromRequest
                    .equals(channelInformation.getServerId())) {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + channelInformation.getServerId() + "/by-name/"
                        + CustomUrlCodec.encode(channelName));
            }
            // Next, we want to get the channel's status. However, we do not
            // consider it fatal if we cannot get the status (most likely the
            // server is simply offline).
            try {
                UUID thisServerId = clusterManagementService.getThisServer()
                        .getServerId();
                if (thisServerId.equals(channelInformation.getServerId())) {
                    channelStatus = archivingService
                            .getChannelStatus(channelName);
                } else {
                    String targetServerUrl = clusterManagementService
                            .getInterNodeCommunicationUrl(
                                    channelInformation.getServerId());
                    channelStatus = FutureUtils
                            .getUnchecked(interNodeCommunicationService
                                    .getArchivingStatusForChannel(
                                            targetServerUrl, channelName));
                }
            } catch (RuntimeException e) {
                // There are many reasons why we might not be able to get the
                // status. The archiving service might not be initialized yet,
                // the target server might not be online, etc.
                // We do not treat this as an error. Instead we fall back to
                // reading the configuration from the database.
            }
        }
        ChannelConfiguration channelConfiguration = null;
        if (channelStatus != null) {
            // The channel status includes the configuration, so we can simply
            // use that information
            channelConfiguration = channelStatus.getChannelConfiguration();
        } else if (channelInformation != null) {
            try {
                channelConfiguration = FutureUtils
                        .getUnchecked(channelMetaDataDAO.getChannelByServer(
                                channelInformation.getServerId(), channelName));
            } catch (RuntimeException e) {
                // If getting the channel configuration fails, we still want to
                // carry on. We set the appropriate response code later.
            }
        }
        String controlSystemName = null;
        if (channelConfiguration != null) {
            ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                    .getControlSystemSupport(
                            channelConfiguration.getControlSystemType());
            if (controlSystemSupport == null) {
                controlSystemName = channelConfiguration.getControlSystemType();
            } else {
                controlSystemName = controlSystemSupport.getName();
            }
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else if (serverIdFromRequest != null) {
            serverId = serverIdFromRequest;
        } else {
            serverId = null;
        }
        String serverName = "unknown";
        if (serverId != null) {
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelConfiguration == null) {
            // If the channel exists (or could exist), but we do not have the
            // configuration, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelConfiguration", channelConfiguration);
        modelMap.put("channelConfigurationAvailable",
                channelConfiguration != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("channelStatus", channelStatus);
        modelMap.put("channelStatusAvailable", channelStatus != null);
        modelMap.put("controlSystemName", controlSystemName);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/details", modelMap);
    }

    private ModelAndView channelEditPrepareForm(String channelName,
            UUID serverIdFromRequest, HttpServletResponse response,
            String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        populateModelWithChannelConfiguration(modelMap, channelName,
                serverIdFromRequest);
        if (!((Boolean) modelMap.get("channelExists"))) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (modelMap.get("channelConfiguration") == null) {
            // If the channel exists (or could exist), but we do not have the
            // configuration, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } else {
            // If the channel belongs to a different server and a server was
            // specified in the URI path, we want to redirect to the correct
            // server.
            ChannelConfiguration channelConfiguration = (ChannelConfiguration) modelMap
                    .get("channelConfiguration");
            if (serverIdFromRequest != null && !serverIdFromRequest
                    .equals(channelConfiguration.getServerId())) {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + channelConfiguration.getServerId() + "/by-name/"
                        + CustomUrlCodec.encode(channelName) + "/edit");
            }
            // We want to populate the form with the data from the existing
            // configuration.
            NavigableMap<Integer, DecimationLevel> decimationLevels = new TreeMap<Integer, DecimationLevel>();
            TreeMap<Integer, Boolean> removeDecimationLevels = new TreeMap<Integer, Boolean>();
            int i = -1;
            for (Map.Entry<Integer, Integer> decimationLevelEntry : channelConfiguration
                    .getDecimationLevelToRetentionPeriod().entrySet()) {
                ++i;
                decimationLevels.put(i,
                        createDecimationLevel(decimationLevelEntry.getKey(),
                                decimationLevelEntry.getValue()));
                removeDecimationLevels.put(i, false);
            }
            NavigableMap<Integer, ControlSystemOption> options = new TreeMap<Integer, ControlSystemOption>();
            TreeMap<Integer, Boolean> removeOptions = new TreeMap<Integer, Boolean>();
            i = -1;
            for (Map.Entry<String, String> optionEntry : channelConfiguration
                    .getOptions().entrySet()) {
                ++i;
                ControlSystemOption option = new ControlSystemOption();
                option.setName(optionEntry.getKey());
                option.setValue(optionEntry.getValue());
                options.put(i, option);
                removeOptions.put(i, false);
            }
            // The form is only accessed by the view and only used for reading
            // data. Therefore, we can use the same instance for the "original"
            // variants of the decimation levels and options. We make the maps
            // unmodifiable so that we can easily detect a bug where they are
            // modified.
            decimationLevels = Maps.unmodifiableNavigableMap(decimationLevels);
            options = Maps.unmodifiableNavigableMap(options);
            EditChannelForm form = new EditChannelForm();
            form.setDecimationLevels(decimationLevels);
            form.setEnabled(channelConfiguration.isEnabled());
            form.setOptions(options);
            form.setOriginalDecimationLevels(decimationLevels);
            form.setOriginalOptions(options);
            form.setRemoveDecimationLevels(removeDecimationLevels);
            form.setRemoveOptions(removeOptions);
            modelMap.put("form", form);
        }
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/edit", modelMap);
    }

    private ModelAndView channelEditProcessForm(String channelName,
            UUID serverIdFromRequest, EditChannelForm form,
            BindingResult formBindingResult, HttpServletResponse response,
            String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        // If there was a validation error, we again show the form to the user.
        if (formBindingResult.hasErrors()) {
            return channelEditProcessFormError(channelName, serverIdFromRequest,
                    form, formBindingResult, response, viewMode);
        }
        // We check whether the specified channel still exists.
        try {
            if (FutureUtils.getUnchecked(channelInformationCache
                    .getChannelAsync(channelName, false)) == null) {
                formBindingResult.reject("NoSuchChannelError",
                        "Channel does not exist.");
            }
        } catch (RuntimeException e) {
            // If we cannot check whether the channel exists, there is no sense
            // in trying to add the channel because this database operation will
            // most likely fail as well.
            String exceptionMessage = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName();
            formBindingResult.reject("DatabaseAccessError",
                    new Object[] { exceptionMessage },
                    "Error while accessing the database.");
        }
        // We have to check for errors again because we might have added some.
        if (formBindingResult.hasErrors()) {
            return channelEditProcessFormError(channelName, serverIdFromRequest,
                    form, formBindingResult, response, viewMode);
        }
        // If the validation was successful, we know that the data in the form
        // is well-formed and thus we can use it without any further checks.
        Set<Integer> decimationLevelsAddedOrUpdated = new HashSet<Integer>();
        Set<Integer> decimationLevelsRemoved = new HashSet<Integer>();
        Map<Integer, Integer> decimationLevelToRetentionPeriod = new HashMap<Integer, Integer>();
        for (Map.Entry<Integer, DecimationLevel> decimationLevelEntry : form
                .getDecimationLevels().entrySet()) {
            Integer i = decimationLevelEntry.getKey();
            DecimationLevel decimationLevel = decimationLevelEntry.getValue();
            if (decimationLevel == null) {
                continue;
            }
            Boolean removeDecimationLevel = form.getRemoveDecimationLevels()
                    .get(i);
            if (removeDecimationLevel != null && removeDecimationLevel) {
                decimationLevelsRemoved.add(
                        decimationLevel.getDecimationPeriod().getSeconds());
            } else {
                Integer decimationPeriodInSeconds = decimationLevel
                        .getDecimationPeriod().getSeconds();
                decimationLevelsAddedOrUpdated.add(decimationPeriodInSeconds);
                decimationLevelToRetentionPeriod.put(decimationPeriodInSeconds,
                        decimationLevel.getRetentionPeriod().getSeconds());
            }
        }
        Map<String, String> optionsAddedOrUpdated = new HashMap<String, String>();
        Set<String> optionsRemoved = new HashSet<String>();
        for (Map.Entry<Integer, ControlSystemOption> optionEntry : form
                .getOptions().entrySet()) {
            Integer i = optionEntry.getKey();
            ControlSystemOption option = optionEntry.getValue();
            if (option == null) {
                continue;
            }
            Boolean removeOption = form.getRemoveOptions().get(i);
            if (removeOption != null && removeOption) {
                optionsRemoved.add(option.getName());
            } else {
                optionsAddedOrUpdated.put(option.getName(), option.getValue());
            }
        }
        try {
            FutureUtils.getUnchecked(archiveConfigurationService.updateChannel(
                    null, channelName, null, null,
                    decimationLevelsAddedOrUpdated, decimationLevelsRemoved,
                    decimationLevelToRetentionPeriod, form.getEnabled(), null,
                    optionsAddedOrUpdated, optionsRemoved));
        } catch (NoSuchChannelException e) {
            formBindingResult.reject("NoSuchChannelError",
                    "Channel does not exist.");
        } catch (PendingChannelOperationException e) {
            String exceptionMessage = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName();
            formBindingResult.reject("PendingChannelOperationError",
                    new Object[] { exceptionMessage },
                    "Another operation is pending.");
        } catch (RuntimeException e) {
            String exceptionMessage = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName();
            formBindingResult.reject("DatabaseAccessError",
                    new Object[] { exceptionMessage },
                    "Error while accessing the database.");
        }
        // If the operation failed, we added an error to the binding result.
        if (formBindingResult.hasErrors()) {
            return channelEditProcessFormError(channelName, serverIdFromRequest,
                    form, formBindingResult, response, viewMode);
        }
        // After successfully updating the channel, we redirect to its details
        // page. Depending on the view-mode, the URL looks slightly different.
        if (serverIdFromRequest == null) {
            return new ModelAndView("redirect:/admin/ui/channels/all/by-name/"
                    + CustomUrlCodec.encode(channelName) + "/?message.updated");
        } else {
            return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                    + serverIdFromRequest.toString() + "/by-name/"
                    + CustomUrlCodec.encode(channelName) + "/?message.updated");
        }
    }

    private ModelAndView channelEditProcessFormError(String channelName,
            UUID serverIdFromRequest, EditChannelForm form,
            BindingResult formBindingResult, HttpServletResponse response,
            String viewMode) {
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        populateModelWithChannelConfiguration(modelMap, channelName,
                serverIdFromRequest);
        if (!((Boolean) modelMap.get("channelExists"))) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (modelMap.get("channelConfiguration") == null) {
            // If the channel exists (or could exist), but we do not have the
            // configuration, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        // If the channel belongs to a different server than the server that was
        // specified in the URI path, we would want to redirect to the correct
        // server. However, unlike when displaying the form initially, we cannot
        // do this without losing all form input. Therefore, we stay with the
        // current path and do the redirect when editing has finished.
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/edit", modelMap);
    }

    private ModelAndView channelMovePrepareForm(String channelName,
            UUID serverIdFromRequest, HttpServletResponse response,
            String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // information available, there must have been a problem retrieving
            // the information.
            channelExists = true;
            channelInformation = null;
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        String serverName = "unknown";
        if (serverId != null) {
            // If the channel belongs to a different server than the one
            // specified as part of the URI path, we redirect to the correct
            // server.
            if (serverIdFromRequest != null
                    && !serverIdFromRequest.equals(serverId)) {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + serverId.toString() + "/by-name/"
                        + CustomUrlCodec.encode(channelName) + "/move");
            }
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        // We want the servers to be ordered by name, but the name might not
        // be unique, so we have to use a multi-map.
        Multimap<String, UUID> servers = Multimaps.newSortedSetMultimap(
                new TreeMap<String, Collection<UUID>>(),
                new Supplier<TreeSet<UUID>>() {
                    @Override
                    public TreeSet<UUID> get() {
                        return new TreeSet<UUID>(UUID_COMPARATOR);
                    }
                });
        for (ServerStatus server : clusterManagementService.getServers()) {
            servers.put(server.getServerName(), server.getServerId());
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelInformation == null) {
            // If the channel exists (or could exist), but we do not have the
            // information, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelInformationAvailable", channelInformation != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("globalError", null);
        modelMap.put("newServerId", serverId);
        modelMap.put("newServerIdError", null);
        modelMap.put("serverChanged", false);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("servers", servers);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/move", modelMap);
    }

    private ModelAndView channelMoveProcessForm(String channelName,
            UUID serverIdFromRequest, UUID oldServerId, UUID newServerId,
            HttpServletResponse response, String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        DefaultMessageSourceResolvable newServerIdError = null;
        if (newServerId == null) {
            newServerIdError = new DefaultMessageSourceResolvable("NotNull");
        }
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // configuration available, there must have been a problem
            // retrieving the information.
            channelExists = true;
            channelInformation = null;
        }
        UUID serverId = null;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        boolean serverChanged = !oldServerId.equals(serverId);
        DefaultMessageSourceResolvable globalError = null;
        boolean moveSuccessful = false;
        if (channelInformation != null && newServerIdError == null
                && !serverChanged) {
            try {
                FutureUtils.getUnchecked(archiveConfigurationService
                        .moveChannel(oldServerId, newServerId, channelName));
                moveSuccessful = true;
            } catch (IllegalArgumentException e) {
                serverChanged = true;
            } catch (NoSuchChannelException e) {
                channelExists = false;
                globalError = new DefaultMessageSourceResolvable(
                        "NoSuchChannelError");
            } catch (PendingChannelOperationException e) {
                globalError = new DefaultMessageSourceResolvable(
                        "PendingChannelOperationError");
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = e.getClass().getName();
                }
                globalError = new DefaultMessageSourceResolvable(
                        new String[] { "DatabaseAccessError" },
                        new Object[] { exceptionMessage });
            }
        }
        // If the move operation was successful, we redirect to the
        // channel-details view.
        if (moveSuccessful) {
            if (serverIdFromRequest == null) {
                return new ModelAndView(
                        "redirect:/admin/ui/channels/all/by-name/"
                                + CustomUrlCodec.encode(channelName)
                                + "/?message.moved");
            } else {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + newServerId.toString() + "/by-name/"
                        + CustomUrlCodec.encode(channelName)
                        + "/?message.moved");
            }
        }
        String serverName = "unknown";
        if (serverId != null) {
            // If the channel belongs to a different server than the one
            // specified as part of the URI path, we redirect to the correct
            // server.
            if (serverIdFromRequest != null
                    && !serverIdFromRequest.equals(serverId)) {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + serverId.toString() + "/by-name/"
                        + CustomUrlCodec.encode(channelName) + "/move");
            }
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        // We want the servers to be ordered by name, but the name might not
        // be unique, so we have to use a multi-map.
        Multimap<String, UUID> servers = Multimaps.newSortedSetMultimap(
                new TreeMap<String, Collection<UUID>>(),
                new Supplier<TreeSet<UUID>>() {
                    @Override
                    public TreeSet<UUID> get() {
                        return new TreeSet<UUID>(UUID_COMPARATOR);
                    }
                });
        for (ServerStatus server : clusterManagementService.getServers()) {
            servers.put(server.getServerName(), server.getServerId());
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelInformation == null) {
            // If the channel exists (or could exist), but we do not have the
            // information, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelInformationAvailable", channelInformation != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("globalError", globalError);
        modelMap.put("newServerId",
                (newServerId == null) ? serverId : newServerId);
        modelMap.put("newServerIdError", newServerIdError);
        modelMap.put("serverChanged", serverChanged);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("servers", servers);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/move", modelMap);
    }

    private ModelAndView channelRemovePrepareForm(String channelName,
            UUID serverIdFromRequest, HttpServletResponse response,
            String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // information available, there must have been a problem retrieving
            // the information.
            channelExists = true;
            channelInformation = null;
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        String serverName = "unknown";
        if (serverId != null) {
            // If the channel belongs to a different server than the one
            // specified as part of the URI path, we redirect to the correct
            // server.
            if (serverIdFromRequest != null
                    && !serverIdFromRequest.equals(serverId)) {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + serverId.toString() + "/by-name/"
                        + CustomUrlCodec.encode(channelName) + "/remove");
            }
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelInformation == null) {
            // If the channel exists (or could exist), but we do not have the
            // information, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelInformationAvailable", channelInformation != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("globalError", null);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/remove", modelMap);
    }

    private ModelAndView channelRemoveProcessForm(String channelName,
            UUID serverIdFromRequest, HttpServletResponse response,
            String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // configuration available, there must have been a problem
            // retrieving the information.
            channelExists = true;
            channelInformation = null;
        }
        DefaultMessageSourceResolvable globalError = null;
        boolean removeSuccessful = false;
        try {
            FutureUtils.getUnchecked(archiveConfigurationService
                    .removeChannel(null, channelName));
            removeSuccessful = true;
        } catch (NoSuchChannelException e) {
            channelExists = false;
            globalError = new DefaultMessageSourceResolvable(
                    "NoSuchChannelError");
        } catch (PendingChannelOperationException e) {
            globalError = new DefaultMessageSourceResolvable(
                    "PendingChannelOperationError");
        } catch (RuntimeException e) {
            String exceptionMessage = e.getMessage();
            if (exceptionMessage == null) {
                exceptionMessage = e.getClass().getName();
            }
            globalError = new DefaultMessageSourceResolvable(
                    new String[] { "DatabaseAccessError" },
                    new Object[] { exceptionMessage });
        }
        // If the remove operation was successful, we redirect to the
        // channels-all or channels-by-server view.
        if (removeSuccessful) {
            if (serverIdFromRequest == null) {
                return new ModelAndView(
                        "redirect:/admin/ui/channels/all/?message.channelDeleted");
            } else {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + serverIdFromRequest.toString()
                        + "/?message.channelDeleted");
            }
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        String serverName = "unknown";
        if (serverId != null) {
            // If the channel belongs to a different server than the one
            // specified as part of the URI path, we still do not redirect
            // because we cannot do this safely for a POST request. However, we
            // display the new server as part of the breadcrumb navigation and
            // redirect after the rename operation has finished.
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelInformation == null || globalError != null) {
            // If the channel exists (or could exist), but we do not have the
            // information, or there was a problem with renaming the channel, we
            // indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelInformationAvailable", channelInformation != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("globalError", globalError);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/remove", modelMap);
    }

    private ModelAndView channelRenamePrepareForm(String channelName,
            UUID serverIdFromRequest, HttpServletResponse response,
            String viewMode) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // information available, there must have been a problem retrieving
            // the information.
            channelExists = true;
            channelInformation = null;
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        String serverName = "unknown";
        if (serverId != null) {
            // If the channel belongs to a different server than the one
            // specified as part of the URI path, we redirect to the correct
            // server.
            if (serverIdFromRequest != null
                    && !serverIdFromRequest.equals(serverId)) {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + serverId.toString() + "/by-name/"
                        + CustomUrlCodec.encode(channelName) + "/rename");
            }
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelInformation == null) {
            // If the channel exists (or could exist), but we do not have the
            // information, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelInformationAvailable", channelInformation != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("globalError", null);
        modelMap.put("newChannelName", channelName);
        modelMap.put("newChannelNameError", null);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/rename", modelMap);
    }

    private ModelAndView channelRenameProcessForm(String oldChannelName,
            UUID serverIdFromRequest, String newChannelName,
            HttpServletResponse response, String viewMode) {
        // The old channel name is encoded, so we have to decode it first.
        oldChannelName = CustomUrlCodec.decode(oldChannelName);
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(oldChannelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // configuration available, there must have been a problem
            // retrieving the information.
            channelExists = true;
            channelInformation = null;
        }
        DefaultMessageSourceResolvable newChannelNameError = null;
        if (newChannelName == null) {
            newChannelNameError = new DefaultMessageSourceResolvable("NotNull");
        } else if (newChannelName.isEmpty()) {
            newChannelNameError = new DefaultMessageSourceResolvable("Size");
        }
        DefaultMessageSourceResolvable globalError = null;
        boolean renameSuccessful = false;
        // If everything looks good so far, we try to rename the channel.
        if (channelExists && channelInformation != null
                && newChannelNameError == null) {
            try {
                FutureUtils.getUnchecked(archiveConfigurationService
                        .renameChannel(null, oldChannelName, newChannelName));
                renameSuccessful = true;
            } catch (ChannelAlreadyExistsException e) {
                newChannelNameError = new DefaultMessageSourceResolvable(
                        "ChannelNameUnique");
            } catch (NoSuchChannelException e) {
                channelExists = false;
                globalError = new DefaultMessageSourceResolvable(
                        "NoSuchChannelError");
            } catch (PendingChannelOperationException e) {
                globalError = new DefaultMessageSourceResolvable(
                        "PendingChannelOperationError");
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = e.getClass().getName();
                }
                globalError = new DefaultMessageSourceResolvable(
                        new String[] { "DatabaseAccessError" },
                        new Object[] { exceptionMessage });
            }
        }
        // If the rename operation was successful, we redirect to the
        // channel-details view.
        if (renameSuccessful) {
            if (serverIdFromRequest == null) {
                return new ModelAndView(
                        "redirect:/admin/ui/channels/all/by-name/"
                                + CustomUrlCodec.encode(newChannelName)
                                + "/?message.renamed");
            } else {
                return new ModelAndView("redirect:/admin/ui/channels/by-server/"
                        + serverIdFromRequest.toString() + "/by-name/"
                        + CustomUrlCodec.encode(newChannelName)
                        + "/?message.renamed");
            }
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        String serverName = "unknown";
        if (serverId != null) {
            // If the channel belongs to a different server than the one
            // specified as part of the URI path, we still do not redirect
            // because we cannot do this safely for a POST request. However, we
            // display the new server as part of the breadcrumb navigation and
            // redirect after the rename operation has finished.
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response. However, we do not send an error because we
            // still want to display the error message in the regular page.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else if (channelInformation == null || globalError != null) {
            // If the channel exists (or could exist), but we do not have the
            // information, or there was a problem with renaming the channel, we
            // indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        HashMap<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("channelInformationAvailable", channelInformation != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", oldChannelName);
        modelMap.put("globalError", globalError);
        modelMap.put("newChannelName",
                (newChannelName == null) ? "" : newChannelName);
        modelMap.put("newChannelNameError", newChannelNameError);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
        modelMap.put("viewMode", viewMode);
        return new ModelAndView("channels/rename", modelMap);
    }

    private DecimationLevel createDecimationLevel(int decimationPeriodInSeconds,
            int retentionPeriodInSeconds) {
        TimePeriod decimationPeriod = createTimePeriod(
                decimationPeriodInSeconds);
        TimePeriod retentionPeriod = createTimePeriod(retentionPeriodInSeconds);
        DecimationLevel decimationLevel = new DecimationLevel();
        decimationLevel.setDecimationPeriod(decimationPeriod);
        decimationLevel.setRetentionPeriod(retentionPeriod);
        return decimationLevel;
    }

    private TimePeriod createTimePeriod(int seconds) {
        TimePeriod timePeriod = new TimePeriod();
        if (seconds == 0) {
            timePeriod.setCounts(0);
            timePeriod.setUnit(TimePeriodUnit.ZERO);
        } else if (seconds % 86400 == 0) {
            timePeriod.setCounts(seconds / 86400);
            timePeriod.setUnit(TimePeriodUnit.DAYS);
        } else if (seconds % 3600 == 0) {
            timePeriod.setCounts(seconds / 3600);
            timePeriod.setUnit(TimePeriodUnit.HOURS);
        } else if (seconds % 60 == 0) {
            timePeriod.setCounts(seconds / 60);
            timePeriod.setUnit(TimePeriodUnit.MINUTES);
        } else {
            timePeriod.setCounts(seconds);
            timePeriod.setUnit(TimePeriodUnit.SECONDS);
        }
        return timePeriod;
    }

    private DecimationLevel defaultRawDecimationLevel() {
        return createDecimationLevel(0, 0);
    }

    private void normalizeAndValidateDecimationLevels(
            AbstractAddEditChannelForm form, Errors errors,
            NavigableMap<Integer, Boolean> removeDecimationLevels) {
        NavigableMap<Integer, DecimationLevel> decimationLevels = form
                .getDecimationLevels();
        // If the list of decimation levels is null or empty, this always is an
        // error.
        if (decimationLevels == null || decimationLevels.isEmpty()) {
            errors.rejectValue("decimationLevels", "RawDecimationLevelExists",
                    "The decimation level for raw samples is missing or is not the first decimation level.");
            return;
        }
        // We want to normalize the list. First, this makes our tests easier and
        // second we want to present the decimation levels to the user in a
        // normalized way in case the form is displayed again. At the same time,
        // we can check for decimation and retention periods that are too large.
        for (Map.Entry<Integer, DecimationLevel> decimationLevelEntry : decimationLevels
                .entrySet()) {
            // We ensure that there is no null key in beforeValidate, , so we
            // can safely do this.
            int i = decimationLevelEntry.getKey();
            DecimationLevel decimationLevel = decimationLevelEntry.getValue();
            if (decimationLevel == null) {
                continue;
            }
            TimePeriod decimationPeriod = decimationLevel.getDecimationPeriod();
            int decimationPeriodInSeconds;
            try {
                decimationPeriodInSeconds = decimationPeriod.getSeconds();
                if (decimationPeriodInSeconds == 0) {
                    decimationPeriod.setCounts(0);
                    // Only the first entry may and must have a zero decimation
                    // period. All other entries must have a non-zero decimation
                    // period. For those entries, we ensure that the period is
                    // not zero because that period would not be display
                    // correctly anyway.
                    if (i == 0) {
                        decimationPeriod.setUnit(TimePeriodUnit.ZERO);
                    } else {
                        if (decimationPeriod.getUnit()
                                .equals(TimePeriodUnit.ZERO)) {
                            decimationPeriod.setUnit(TimePeriodUnit.SECONDS);
                        }
                    }
                } else if (decimationPeriodInSeconds % 86400 == 0) {
                    decimationPeriod
                            .setCounts(decimationPeriodInSeconds / 86400);
                    decimationPeriod.setUnit(TimePeriodUnit.DAYS);
                } else if (decimationPeriodInSeconds % 3600 == 0) {
                    decimationPeriod
                            .setCounts(decimationPeriodInSeconds / 3600);
                    decimationPeriod.setUnit(TimePeriodUnit.HOURS);
                } else if (decimationPeriodInSeconds % 60 == 0) {
                    decimationPeriod.setCounts(decimationPeriodInSeconds / 60);
                    decimationPeriod.setUnit(TimePeriodUnit.MINUTES);
                } else {
                    // The only way how we can end up in this branch is if the
                    // unit is already seconds, so we do not have to do
                    // anything.
                }
            } catch (IllegalStateException e) {
                // An IllegalStateException is thrown if the specified number of
                // counts is too large for the selected time unit or if it is
                // negative. If it is negative, this is also caught by the
                // annotation-based validation, so we only add an error if it is
                // positive.
                if (decimationPeriod.getCounts() > 0) {
                    errors.rejectValue(
                            "decimationLevels[" + i
                                    + "].decimationPeriod.counts",
                            "Max",
                            new Object[] { decimationPeriod.getCounts() },
                            "number too large");
                }
            } catch (NullPointerException e) {
                // A NullPointerException can occur if a decimation level has
                // not been fully initialized. Actually, this kind of error has
                // already been called by the annotation-based validation, so we
                // do not have to take care of it.
            }
            TimePeriod retentionPeriod = decimationLevel.getRetentionPeriod();
            int retentionPeriodInSeconds;
            try {
                retentionPeriodInSeconds = retentionPeriod.getSeconds();
                if (retentionPeriodInSeconds == 0) {
                    retentionPeriod.setCounts(0);
                    retentionPeriod.setUnit(TimePeriodUnit.ZERO);
                } else if (retentionPeriodInSeconds % 86400 == 0) {
                    retentionPeriod.setCounts(retentionPeriodInSeconds / 86400);
                    retentionPeriod.setUnit(TimePeriodUnit.DAYS);
                } else if (retentionPeriodInSeconds % 3600 == 0) {
                    retentionPeriod.setCounts(retentionPeriodInSeconds / 3600);
                    retentionPeriod.setUnit(TimePeriodUnit.HOURS);
                } else if (retentionPeriodInSeconds % 60 == 0) {
                    retentionPeriod.setCounts(retentionPeriodInSeconds / 60);
                    retentionPeriod.setUnit(TimePeriodUnit.MINUTES);
                } else {
                    // The only way how we can end up in this branch is if the
                    // unit is already seconds, so we do not have to do
                    // anything.
                }
            } catch (IllegalStateException e) {
                // An IllegalStateException is thrown if the specified number of
                // counts is too large for the selected time unit or if it is
                // negative. If it is negative, this is also caught by the
                // annotation-based validation, so we only add an error if it is
                // positive.
                if (retentionPeriod.getCounts() > 0) {
                    errors.rejectValue(
                            "decimationLevels[" + i
                                    + "].retentionPeriod.counts",
                            "Max", new Object[] { retentionPeriod.getCounts() },
                            "number too large");
                }
            } catch (NullPointerException e) {
                // A NullPointerException can occur if a decimation level has
                // not been fully initialized. Actually, this kind of error has
                // already been called by the annotation-based validation, so we
                // do not have to take care of it.
            }
        }
        // The first element always has to be the decimation level for raw
        // samples and this decimation level may never be removed.
        DecimationLevel firstDecimationLevel = decimationLevels.get(0);
        if (firstDecimationLevel == null
                || firstDecimationLevel.getDecimationPeriod()
                        .getCounts() == null
                || firstDecimationLevel.getDecimationPeriod().getCounts() != 0
                || (removeDecimationLevels != null
                        && removeDecimationLevels.get(0) != null
                        && removeDecimationLevels.get(0))) {
            errors.rejectValue("decimationLevels", "RawDecimationLevelExists",
                    "The decimation level for raw samples is missing or is not the first decimation level.");
            return;
        }
        HashSet<Integer> seenDecimationPeriods = new HashSet<Integer>();
        for (Map.Entry<Integer, DecimationLevel> decimationLevelEntry : decimationLevels
                .entrySet()) {
            // We ensure that there is no null key in beforeValidate, , so we
            // can safely do this.
            int i = decimationLevelEntry.getKey();
            DecimationLevel decimationLevel = decimationLevelEntry.getValue();
            if (decimationLevel == null) {
                continue;
            }
            Integer decimationPeriodInSeconds;
            try {
                decimationPeriodInSeconds = decimationLevel
                        .getDecimationPeriod().getSeconds();
            } catch (IllegalStateException | NullPointerException e) {
                // If there is another problem with an entry, we might catch an
                // IllegalStateException or NullPointerException. In this case,
                // a corresponding error has already been set for the field and
                // we can continue with the next entry.
                continue;
            }
            if (decimationPeriodInSeconds == null) {
                // If the entry has not been initialized completely, the period
                // may be null. We simply ignore such an entry because this
                // problem has already been caught by the annotation-based
                // validation.
                continue;
            } else if (decimationPeriodInSeconds == 0 && i != 0) {
                errors.rejectValue(
                        "decimationLevels[" + i + "].decimationPeriod.counts",
                        "Min",
                        "The decimation period must be strictly positive.");
            } else if (seenDecimationPeriods
                    .contains(decimationPeriodInSeconds)) {
                errors.rejectValue(
                        "decimationLevels[" + i + "].decimationPeriod.counts",
                        "UniqueDecimationPeriod",
                        "The decimation period must be unique.");
            } else {
                seenDecimationPeriods.add(decimationPeriodInSeconds);
            }
        }
        // We also have to verify that the retention periods are well-ordered,
        // meaning that each decimation level must have a retention period that
        // is greater than or equal to the one of the preceding (in decimation
        // periods, not index numbers) decimation level.
        // We only run this part of the validation if we have not detected any
        // other errors. This helps us to keep the code simpler because we do
        // not have to worry about negative numbers, etc.
        if (!errors.hasErrors()) {
            validateDecimationLevelRetentionPeriods(decimationLevels, errors);
        }
    }

    private void populateModelWithChannelConfiguration(
            Map<String, Object> modelMap, String channelName,
            UUID serverIdFromRequest) {
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // we cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // configuration available, there must have been a problem
            // retrieving the information.
            channelExists = true;
            channelInformation = null;
        }
        ChannelConfiguration channelConfiguration = null;
        if (channelInformation != null) {
            try {
                channelConfiguration = FutureUtils
                        .getUnchecked(channelMetaDataDAO.getChannelByServer(
                                channelInformation.getServerId(), channelName));
            } catch (RuntimeException e) {
                // If getting the channel configuration fails, we still want to
                // carry on. We set the appropriate response code later.
            }
        }
        String controlSystemName = null;
        if (channelConfiguration != null) {
            ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                    .getControlSystemSupport(
                            channelConfiguration.getControlSystemType());
            if (controlSystemSupport == null) {
                controlSystemName = channelConfiguration.getControlSystemType();
            } else {
                controlSystemName = controlSystemSupport.getName();
            }
        }
        UUID serverId;
        if (channelInformation != null) {
            serverId = channelInformation.getServerId();
        } else {
            serverId = serverIdFromRequest;
        }
        String serverName = "unknown";
        if (serverId != null) {
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverId);
            if (serverStatus != null) {
                serverName = serverStatus.getServerName();
            }
        }
        modelMap.put("channelConfiguration", channelConfiguration);
        modelMap.put("channelConfigurationAvailable",
                channelConfiguration != null);
        modelMap.put("channelExists", channelExists);
        modelMap.put("channelName", channelName);
        modelMap.put("controlSystemName", controlSystemName);
        modelMap.put("serverId", serverId);
        modelMap.put("serverName", serverName);
    }

    private void populateModelWithControlSystemTypes(
            Map<String, Object> modelMap) {
        // We want to add the list of available control-system supports to the
        // model so that the user gets a list to choose from. We want this list
        // to be sorted by the name of the control-system supports (because this
        // is what the user sees), but in theory there could be two
        // control-system supports with the same name but different IDs, so we
        // have to use a multi-map.
        SetMultimap<String, String> controlSystemSupports = Multimaps
                .newSetMultimap(new TreeMap<String, Collection<String>>(),
                        new Supplier<TreeSet<String>>() {
                            @Override
                            public TreeSet<String> get() {
                                return new TreeSet<String>();
                            }
                        });
        for (ControlSystemSupport<?> controlSystemSupport : controlSystemSupportRegistry
                .getControlSystemSupports()) {
            controlSystemSupports.put(controlSystemSupport.getName(),
                    controlSystemSupport.getId());
        }
        modelMap.put("controlSystemSupports", controlSystemSupports);
    }

    private void populateModelWithServerInformation(
            Map<String, Object> modelMap, UUID serverIdFromRequest) {
        // If no server ID was specified as part of the request path, we let the
        // user choose the server. If the server ID was part of the request
        // path, we verify that the server exists. We also make the serverName
        // available so that it can be rendered in the view.
        if (serverIdFromRequest == null) {
            // We want the servers to be ordered by name, but the name might not
            // be unique, so we have to use a multi-map.
            Multimap<String, UUID> servers = Multimaps.newSortedSetMultimap(
                    new TreeMap<String, Collection<UUID>>(),
                    new Supplier<TreeSet<UUID>>() {
                        @Override
                        public TreeSet<UUID> get() {
                            return new TreeSet<UUID>(UUID_COMPARATOR);
                        }
                    });
            for (ServerStatus server : clusterManagementService.getServers()) {
                servers.put(server.getServerName(), server.getServerId());
            }
            modelMap.put("servers", servers);
        } else {
            modelMap.put("serverId", serverIdFromRequest);
            ServerStatus serverStatus = clusterManagementService
                    .getServer(serverIdFromRequest);
            if (serverStatus == null) {
                modelMap.put("serverName", "unknown");
                try {
                    modelMap.put("serverExists", !channelInformationCache
                            .getChannels(serverIdFromRequest).isEmpty());
                } catch (IllegalStateException e) {
                    // If we cannot get the channels for the server because the
                    // cache is not ready, we simply assume that the server
                    // exists. In the worst case, we will let the user add a
                    // channel for a non-existing server ID, which is completely
                    // harmless.
                    modelMap.put("serverExists", true);
                }
            } else {
                modelMap.put("serverExists", true);
                modelMap.put("serverName", serverStatus.getServerName());
            }
        }
    }

    private void validateDecimationLevelRetentionPeriods(
            NavigableMap<Integer, DecimationLevel> decimationLevels,
            Errors errors) {
        // This method is only called after most of the properties have already
        // been validated. Therefore, we do not have to worry about null values
        // or negative numbers.
        // The decimation levels are not sorted by their decimation period, but
        // by their artificial index. We create a map that sorts them by their
        // decimation period but keeps the artificial index so that we can
        // assign the error messages to the right fields. In this map, we use
        // the decimation period as the key and a pair with the retention period
        // and the index as the value.
        TreeMap<Integer, Pair<Integer, Integer>> sortedDecimationLevels = new TreeMap<Integer, Pair<Integer, Integer>>();
        for (Map.Entry<Integer, DecimationLevel> decimationLevelEntry : decimationLevels
                .entrySet()) {
            Integer index = decimationLevelEntry.getKey();
            DecimationLevel decimationLevel = decimationLevelEntry.getValue();
            // We can safely use the getSeconds() method because the decimation
            // levels have already been validated and normalized.
            sortedDecimationLevels.put(
                    decimationLevel.getDecimationPeriod().getSeconds(),
                    Pair.of(decimationLevel.getRetentionPeriod().getSeconds(),
                            index));
        }
        Integer lastDecimationPeriod = null;
        Integer lastRetentionPeriod = null;
        Integer lastIndex = null;
        for (Map.Entry<Integer, Pair<Integer, Integer>> entry : sortedDecimationLevels
                .entrySet()) {
            Integer decimationPeriod = entry.getKey();
            Integer retentionPeriod = entry.getValue().getLeft();
            Integer index = entry.getValue().getRight();
            // We only check one of the three variables because we always set
            // all of them together.
            if (lastDecimationPeriod == null) {
                lastDecimationPeriod = decimationPeriod;
                lastRetentionPeriod = retentionPeriod;
                lastIndex = index;
                continue;
            }
            if (retentionPeriod.intValue() != 0
                    && (lastRetentionPeriod.intValue() == 0
                            || retentionPeriod < lastRetentionPeriod)) {
                DecimationLevel decimationLevel = decimationLevels.get(index);
                DecimationLevel lastDecimationLevel = decimationLevels
                        .get(lastIndex);
                errors.rejectValue(
                        "decimationLevels[" + index + "].retentionPeriod",
                        "NotLessThanPreceding",
                        new Object[] {
                                decimationLevel.getDecimationPeriod()
                                        .getCounts(),
                                decimationLevel.getDecimationPeriod().getUnit()
                                        .ordinal(),
                                decimationLevel.getRetentionPeriod()
                                        .getCounts(),
                                decimationLevel.getRetentionPeriod().getUnit()
                                        .ordinal(),
                                lastDecimationLevel.getDecimationPeriod()
                                        .getCounts(),
                                lastDecimationLevel.getDecimationPeriod()
                                        .getUnit().ordinal(),
                                lastDecimationLevel.getRetentionPeriod()
                                        .getCounts(),
                                lastDecimationLevel.getRetentionPeriod()
                                        .getUnit().ordinal() },
                        "The retention period of {2,choice,0#unbounded|0<'{2} {3,choice,1#second|2#minute|3#hour|4#day}{2,choice,0#s|1#|1<s}'} is too short for the {0,choice,0#raw samples|0<'decimation level with a decimation period of {0} {1,choice,1#second|2#minute|3#hour|4#day}{0,choice,0#s|1#|1<s}'} because it must not be less than the retention period of the {4,choice,0#raw samples|0<'decimation level with a decimation period of {4} {5,choice,1#second|2#minute|3#hour|4#day}{4,choice,0#s|1#|1<s}'} which has a retention period of {6,choice,0#unbounded|0<'{6} {7,choice,1#second|2#minute|3#hour|4#day}{6,choice,0#s|1#|1<s}'}.");
            }
            lastDecimationPeriod = decimationPeriod;
            lastRetentionPeriod = retentionPeriod;
            lastIndex = index;
        }
    }

    private void validateOptions(AbstractAddEditChannelForm form,
            Errors errors) {
        NavigableMap<Integer, ControlSystemOption> options = form.getOptions();
        if (options == null || options.isEmpty()) {
            return;
        }
        HashSet<String> seenOptionNames = new HashSet<String>();
        for (Map.Entry<Integer, ControlSystemOption> optionEntry : options
                .entrySet()) {
            // We ensured in beforeValidate that there is no null key, so this
            // is safe.
            int i = optionEntry.getKey();
            ControlSystemOption option = optionEntry.getValue();
            if (option == null) {
                continue;
            }
            String optionName = option.getName();
            if (optionName == null) {
                // If the option name is null, the annotation-based validation
                // already caught this and generated a corresponding error, so
                // we can simply ignore this entry.
                continue;
            } else if (seenOptionNames.contains(optionName)) {
                errors.rejectValue("options[" + i + "].name",
                        "UniqueOptionName", "The option name must be unique.");
            } else {
                seenOptionNames.add(optionName);
            }
        }
    }

}
