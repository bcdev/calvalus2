/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.Widget;

/**
 * Uploading (and maybe more...) for mask data
 */
public class ManageMasksView extends PortalView {


    private ManageMasksForm manageMasksForm;

    public ManageMasksView(PortalContext portalContext) {
        super(portalContext);
        manageMasksForm = new ManageMasksForm(portalContext);
    }

    @Override
    public Widget asWidget() {
        return manageMasksForm;
    }

    @Override
    public String getTitle() {
        return "Masks";
    }

}
