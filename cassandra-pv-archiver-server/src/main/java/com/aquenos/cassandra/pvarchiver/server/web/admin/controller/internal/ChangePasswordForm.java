/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.UiController;

/**
 * <p>
 * Form-backing bean for changing the user's password.
 * </p>
 * 
 * <p>
 * This class is only intended for use by the {@link UiController} and its
 * associated views.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChangePasswordForm {

    @NotNull
    private String oldPassword;
    @NotNull
    @Pattern(regexp = "\\S*")
    @Size(min = 1)
    private String newPassword;
    @NotNull
    private String newPasswordRepeated;

    /**
     * Returns the old (plain-text) password. This password is used to verify
     * that the user is actually permitted to change the password.
     * 
     * @return old password entered by the user or <code>null</code> if this
     *         bean has not been properly initialized yet.
     */
    public String getOldPassword() {
        return oldPassword;
    }

    /**
     * Sets the old (plain-text) password. This password is used to verify that
     * the user is actually permitted to change the password. A
     * <code>null</code> value is acceptable but will later lead to a validation
     * error.
     * 
     * @param oldPassword
     *            old password entered by the user (may be <code>null</code>).
     */
    public void setOldPassword(String oldPassword) {
        this.oldPassword = oldPassword;
    }

    /**
     * Returns the new (plain-text) password. This is the password that will be
     * used as the new password of the user after the change process has
     * succeeded.
     * 
     * @return new password entered by the user or <code>null</code> if this
     *         bean has not been properly initialized yet.
     */
    public String getNewPassword() {
        return newPassword;
    }

    /**
     * Sets the new (plain-text) password. This is the password that will be
     * used as the new password of the user after the change process has
     * succeeded. A <code>null</code> or empty value is acceptable but will
     * later lead to a validation error. If the password contains white space,
     * this will also lead to a validation error.
     * 
     * @param newPassword
     *            new password entered by the user (may be <code>null</code>).
     */
    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    /**
     * Returns the repeated new (plain-text) password. This password is compared
     * with the new password (as returned by {@link #getNewPassword()} in order
     * to verify that the user did not mistype.
     * 
     * @return repeated new password enetered by the user or <code>null</code>
     *         if this bean has not been properly initialized yet.
     */
    public String getNewPasswordRepeated() {
        return newPasswordRepeated;
    }

    /**
     * Sets the repeated new (plain-text) password. This password is compared
     * with the new password (set with {@link #setNewPassword(String)} in order
     * to verify that the user did not mistype. A <code>null</code> value is
     * acceptable but will later lead to a validation error.
     * 
     * @param newPasswordRepeated
     *            repeated new password enetered by the user (may be
     *            <code>null</code>).
     */
    public void setNewPasswordRepeated(String newPasswordRepeated) {
        this.newPasswordRepeated = newPasswordRepeated;
    }

}
