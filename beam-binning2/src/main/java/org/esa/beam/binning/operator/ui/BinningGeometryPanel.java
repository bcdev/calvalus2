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

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.ui.AppContext;
import org.esa.beam.framework.ui.crs.CrsForm;
import org.esa.beam.framework.ui.crs.CrsSelectionPanel;
import org.esa.beam.framework.ui.crs.PredefinedCrsForm;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

/**
 * The panel in the binning operator UI which allows for setting the geometry.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
class BinningGeometryPanel extends JPanel {

    private final AppContext appContext;
    private final BinningModel model;
    private CrsSelectionPanel crsSelectionPanel;

    BinningGeometryPanel(AppContext appContext, BinningModel model) {
        this.appContext = appContext;
        this.model = model;
        final BorderLayout layout = new BorderLayout(4, 4);
        setLayout(layout);
        init();
    }

    private void init() {
        final PredefinedCrsForm predefinedCrsForm = new PredefinedCrsForm(appContext);

        crsSelectionPanel = new CrsSelectionPanel(predefinedCrsForm, new NullCrsForm(appContext));
        crsSelectionPanel.prepareShow();
        crsSelectionPanel.addPropertyChangeListener("crs", new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                updateCRS();
            }
        });
        add(crsSelectionPanel, BorderLayout.NORTH);
    }

    private void updateCRS() {
        try {
            final CoordinateReferenceSystem crs = crsSelectionPanel.getCrs(null);
            if (crs != null) {
                model.setProperty(BinningModel.PROPERTY_KEY_CRS, crs.toWKT());
            } else {
                model.setProperty(BinningModel.PROPERTY_KEY_CRS, null);
            }
        } catch (Exception e) {
            appContext.handleError("Unable to update CRS", e);
        }
    }

    private static class NullCrsForm extends CrsForm {

        protected NullCrsForm(AppContext appContext) {
            super(appContext);
        }

        @Override
        protected String getLabelText() {
            return "<html>Compute the geographical<br>" +
                   "region according to extents<br>" +
                   "of input products</html>";
        }

        @Override
        public CoordinateReferenceSystem getCRS(GeoPos referencePos) throws FactoryException {
            return null;
        }

        @Override
        protected JComponent createCrsComponent() {
            return new JLabel("");
        }

        @Override
        public void prepareShow() {
        }

        @Override
        public void prepareHide() {
        }
    }
}
