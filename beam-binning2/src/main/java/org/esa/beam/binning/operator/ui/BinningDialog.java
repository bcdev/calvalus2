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

import org.esa.beam.binning.operator.BinningOp;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.ui.SingleTargetProductDialog;
import org.esa.beam.framework.ui.AppContext;

import java.util.HashMap;
import java.util.Map;

/**
 * UI for binning operator.
 *
 * todo
 * - target product: remove outer group [done]
 * - remove first bullet, replace by min/max lat/lon or beam map region chooser
 * - valid expression globally [done]
 * - allow specifying bands by expression
 * - weight -> parameters, empty by default, add description on aggregators to help
 * - fill value -> NaN by default
 * - missing parameters: add to parameters panel
 * - remove formatter config
 * - geometry: rename to region
 * - add time parameters
 * - allow specifying spatial resolution OR supersampling
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
public class BinningDialog extends SingleTargetProductDialog {

    private BinningForm form;
    private final BinningModel model;

    protected BinningDialog(AppContext appContext, String title, String helpID) {
        super(appContext, title, helpID);
        model = new BinningModelImpl();
        form = new BinningForm(appContext, model, getTargetProductSelector());
    }

    @Override
    protected Product createTargetProduct() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().addOperatorSpi(new BinningOp.Spi());

        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("region", model.getRegion());
        return GPF.createProduct("Binning", parameters, model.getSourceProducts());
    }

    @Override
    public int show() {
        setContent(form);
        return super.show();
    }

}
