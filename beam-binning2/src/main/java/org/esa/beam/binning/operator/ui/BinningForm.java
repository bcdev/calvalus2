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

import org.esa.beam.framework.gpf.ui.TargetProductSelector;
import org.esa.beam.framework.ui.AppContext;

import javax.swing.JPanel;
import javax.swing.JTabbedPane;

/**
 * The form for the {@link BinningDialog}.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
class BinningForm extends JTabbedPane {

    private final JPanel ioPanel;
    private final JPanel geometryPanel;
    private final JPanel binningConfigPanel;
    private final JPanel formatterConfigPanel;

    BinningForm(AppContext appContext, BinningModel binningModel, TargetProductSelector targetProductSelector) {
        ioPanel = new BinningIOPanel(appContext, binningModel, targetProductSelector);
        geometryPanel = new BinningGeometryPanel(appContext, binningModel);
        binningConfigPanel = new BinningConfigPanel();
        formatterConfigPanel = new FormatterConfigPanel();
        addTab("I/O Parameters", ioPanel);
        addTab("Geometry", geometryPanel);
        addTab("Binning Config", binningConfigPanel);
        addTab("Formatter Config", formatterConfigPanel);
    }

}
