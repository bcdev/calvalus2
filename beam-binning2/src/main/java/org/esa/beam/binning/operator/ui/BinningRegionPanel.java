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

import com.bc.ceres.swing.TableLayout;
import com.bc.ceres.swing.binding.BindingContext;
import com.bc.ceres.swing.binding.internal.AbstractButtonAdapter;
import org.esa.beam.framework.ui.BoundsInputPanel;
import org.esa.beam.framework.ui.WorldMapPaneDataModel;

import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

/**
 * The panel in the binning operator UI which allows for setting the region.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
class BinningRegionPanel extends JPanel {

    private final BindingContext bindingContext;
    private final WorldMapPaneDataModel worldMapModel;

    BinningRegionPanel(final BinningModel model) {
        bindingContext = model.getBindingContext();
        worldMapModel = new WorldMapPaneDataModel();
        init();
    }

    private void init() {
        final TableLayout layout = new TableLayout(1);
        layout.setTableFill(TableLayout.Fill.BOTH);
        setLayout(layout);

        final JPanel boundsInputPanel = new BoundsInputPanel(bindingContext, BinningModel.PROPERTY_KEY_ENABLE).createBoundsInputPanel(false);

        ButtonGroup buttonGroup = new ButtonGroup();

        final JRadioButton computeOption = new JRadioButton("Compute the geographical region according to extents of input products");
        final JRadioButton globalOption = new JRadioButton("Use the whole globe as region");
        final JRadioButton regionOption = new JRadioButton("Specify region:");

        bindingContext.bind(BinningModel.PROPERTY_KEY_COMPUTE_REGION, new RadioButtonAdapter(computeOption));
        bindingContext.bind(BinningModel.PROPERTY_KEY_GLOBAL, new RadioButtonAdapter(globalOption));
        bindingContext.bind(BinningModel.PROPERTY_KEY_ENABLE, new RadioButtonAdapter(regionOption));

        buttonGroup.add(computeOption);
        buttonGroup.add(globalOption);
        buttonGroup.add(regionOption);

        regionOption.setSelected(true);

        // todo - comment in following code after moving this module to BEAM
        // until then, world map creation will fail because it tries reading the world map images from a jar
        // see MosaicOp UI for how it is done
//        final WorldMapPane worldMapPanel = new WorldMapPane(worldMapModel);
//        worldMapPanel.setMinimumSize(new Dimension(250, 125));
//        worldMapPanel.setBorder(BorderFactory.createEtchedBorder());

        add(computeOption);
        add(globalOption);
        add(regionOption);

        add(boundsInputPanel);
//        add(worldMapPanel);
    }

    public class RadioButtonAdapter extends AbstractButtonAdapter implements ItemListener {

        public RadioButtonAdapter(AbstractButton button) {
            super(button);
        }

        @Override
        public void bindComponents() {
            getButton().addItemListener(this);
        }

        @Override
        public void unbindComponents() {
            getButton().removeItemListener(this);
        }

        @Override
        public void itemStateChanged(ItemEvent e) {
            getBinding().setPropertyValue(getButton().isSelected());
        }
    }
}
