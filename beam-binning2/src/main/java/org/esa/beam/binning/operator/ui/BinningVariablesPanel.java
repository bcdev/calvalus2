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

import com.bc.ceres.binding.ValidationException;
import org.esa.beam.binning.aggregators.AggregatorAverage;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.ui.AppContext;
import org.esa.beam.framework.ui.GridBagUtils;
import org.esa.beam.framework.ui.ModalDialog;
import org.esa.beam.framework.ui.product.ProductExpressionPane;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

/**
 * The panel in the binning operator UI which allows for specifying the configuration of binning variables.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
class BinningVariablesPanel extends JPanel {

    private final AppContext appContext;
    private final BinningModel binningModel;
    private VariableConfigTable bandsTable;

    BinningVariablesPanel(AppContext appContext, BinningModel binningModel) {
        this.appContext = appContext;
        this.binningModel = binningModel;
        setLayout(new BorderLayout());
        add(createBandsPanel(), BorderLayout.CENTER);
        add(createValidExpressionPanel(), BorderLayout.SOUTH);
//        add(createTemporalFilterPanel());
//        add(createSuperSamplingPanel());
    }

    private JPanel createBandsPanel() {
        bandsTable = new VariableConfigTable(binningModel, appContext);
        final JPanel bandsPanel = new JPanel(new GridBagLayout());

        final GridBagConstraints constraints = new GridBagConstraints();
        constraints.insets = new Insets(5, 5, 5, 5);

        constraints.gridx = 0;
        constraints.gridy = 0;
        constraints.gridwidth = 1;
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.weightx = 1.0;

        bandsPanel.add(new JLabel(), constraints);

        constraints.gridx = 1;
        constraints.gridy = 0;
        constraints.gridwidth = 1;
        constraints.weightx = 0.0;
        constraints.fill = GridBagConstraints.NONE;

        final JButton addButton = new JButton("Add");
        addButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                bandsTable.addRow("<expression>", null, AggregatorAverage.Descriptor.NAME, Double.NaN, Double.NaN);
            }
        });
        bandsPanel.add(addButton, constraints);

        constraints.gridx = 2;
        constraints.gridy = 0;
        constraints.weightx = 0.0;

        final JButton removeButton = new JButton("Remove");
        removeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                bandsTable.removeSelectedRows();
            }
        });
        bandsPanel.add(removeButton, constraints);

        constraints.gridx = 0;
        constraints.gridy = 1;
        constraints.gridwidth = 3;
        constraints.fill = GridBagConstraints.BOTH;

        bandsPanel.add(bandsTable.getComponent(), constraints);
        return bandsPanel;
    }

    private JPanel createValidExpressionPanel() {
        JPanel validExpressionPanel = new JPanel(new BorderLayout());
        final JButton button = new JButton("...");
        final Dimension preferredSize = button.getPreferredSize();
        preferredSize.setSize(25, preferredSize.getHeight());
        button.setPreferredSize(preferredSize);
        button.setEnabled(hasSourceProducts());
        binningModel.addPropertyChangeListener(new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                if (!evt.getPropertyName().equals(BinningModel.PROPERTY_KEY_SOURCE_PRODUCTS)) {
                    return;
                }
                button.setEnabled(hasSourceProducts());
            }
        });
        final JTextField textField = new JTextField();
        button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                final String expression = editExpression(textField.getText());
                if (expression != null) {
                    textField.setText(expression);
                    try {
                        binningModel.setProperty(BinningModel.PROPERTY_KEY_EXPRESSION, expression);
                    } catch (ValidationException e) {
                        appContext.handleError("Invalid expression", e);
                    }
                }
            }
        });

        JPanel editorComponent = new JPanel(new BorderLayout());
        editorComponent.add(textField);
        editorComponent.add(button, BorderLayout.EAST);
        validExpressionPanel.add(new JLabel("Valid expression:"), BorderLayout.WEST);
        validExpressionPanel.add(editorComponent);
        return validExpressionPanel;
    }

    private Component createTemporalFilterPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        JLabel label = new JLabel("Temporal Filter");
        JRadioButton noFilter = new JRadioButton("No filter");
        JRadioButton filter = new JRadioButton("By date range");
        JLabel startDateLabel = new JLabel("    Start date:");
        JLabel endDateLabel = new JLabel("    End date:");
        JTextField startDateField = new JTextField();
        JTextField endDateField = new JTextField();

        GridBagConstraints gbc = GridBagUtils.createDefaultConstraints();

        GridBagUtils.addToPanel(panel, label, gbc, "");
        GridBagUtils.addToPanel(panel, noFilter, gbc, "gridy=1");
        GridBagUtils.addToPanel(panel, filter, gbc, "gridy=2");
        GridBagUtils.addToPanel(panel, startDateLabel, gbc, "gridy=3");
        GridBagUtils.addToPanel(panel, startDateField, gbc, "gridx=1, gridy=3");
        GridBagUtils.addToPanel(panel, endDateLabel, gbc, "gridy=4");
        GridBagUtils.addToPanel(panel, endDateField, gbc, "gridx=1, gridy=4");
        return panel;
    }

    private Component createSuperSamplingPanel() {
        return new JPanel();
    }

    private boolean hasSourceProducts() {
        return binningModel.getSourceProducts().length > 0;
    }

    private String editExpression(String expression) {
        final Product product = binningModel.getSourceProducts()[0];
        if (product == null) {
            return null;
        }
        final ProductExpressionPane expressionPane;
        expressionPane = ProductExpressionPane.createBooleanExpressionPane(new Product[]{product}, product,
                                                                           appContext.getPreferences());
        expressionPane.setCode(expression);
        final int i = expressionPane.showModalDialog(appContext.getApplicationWindow(), "Expression Editor");
        if (i == ModalDialog.ID_OK) {
            return expressionPane.getCode();
        }
        return null;
    }

}
