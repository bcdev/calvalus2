/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package org.esa.beam.binning.operator.ui;

import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.ui.SingleTargetProductDialog;
import org.esa.beam.framework.ui.AppContext;

/**
 * UI for binning operator.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
public class BinningDialog extends SingleTargetProductDialog {

    private BinningForm form;
    private final BinningModel model;

    protected BinningDialog(AppContext appContext, String title, String helpID) {
        super(appContext, title, helpID);
        model = new BinningModel();
        form = new BinningForm(appContext, model, getTargetProductSelector());
    }

    @Override
    protected Product createTargetProduct() throws Exception {
        return GPF.createProduct("Binning", model.getParameters(), model.getSourceProducts());
    }

    @Override
    public int show() {
        setContent(form);
        return super.show();
    }

}
