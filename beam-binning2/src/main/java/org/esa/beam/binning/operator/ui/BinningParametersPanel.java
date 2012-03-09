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
import com.bc.ceres.swing.TableLayout;
import org.esa.beam.binning.AggregatorDescriptor;
import org.esa.beam.binning.AggregatorDescriptorRegistry;
import org.esa.beam.binning.aggregators.AggregatorAverage;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.ui.AppContext;
import org.esa.beam.framework.ui.ModalDialog;
import org.esa.beam.framework.ui.UIUtils;
import org.esa.beam.framework.ui.product.BandChooser;
import org.esa.beam.framework.ui.product.ProductExpressionPane;
import org.esa.beam.framework.ui.tool.ToolButtonFactory;
import org.esa.beam.util.StringUtils;

import javax.swing.AbstractButton;
import javax.swing.AbstractCellEditor;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The panel in the binning operator UI which allows for specifying the binning configuration.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
class BinningParametersPanel extends JPanel {

    private final AppContext appContext;
    private final BinningModel model;
    private final String[] aggregatorNames;
    private final List<Component> components;
    private ProcessingParamsTable table;

    BinningParametersPanel(AppContext appContext, BinningModel model) {
        this.appContext = appContext;
        this.model = model;
        model.addPropertyChangeListener(new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                updateComponents();
            }
        });
        this.components = new ArrayList<Component>();
        setLayout(new BorderLayout());
        final AggregatorDescriptor[] aggregatorDescriptors = AggregatorDescriptorRegistry.getInstance().getAggregatorDescriptors();
        aggregatorNames = new String[aggregatorDescriptors.length];
        for (int i = 0; i < aggregatorDescriptors.length; i++) {
            aggregatorNames[i] = aggregatorDescriptors[i].getName();
        }
        final JPanel bandsPanel = createBandsPanel();
        add(bandsPanel);
        updateComponents();
    }

    private void updateComponents() {
        boolean hasSourceProducts = false;
        try {
            hasSourceProducts = model.getSourceProducts().length > 0;
        } catch (IOException e) {
            appContext.handleError("", e);
        }

        for (Component component : components) {
            component.setEnabled(hasSourceProducts);
        }
        if (!hasSourceProducts) {
            table.clear();
        }
    }

    private JPanel createBandsPanel() {
        final TableLayout layout = new TableLayout(3);
        layout.setRowFill(0, TableLayout.Fill.HORIZONTAL);
        layout.setRowFill(1, TableLayout.Fill.BOTH);
        layout.setCellColspan(1, 0, 3);
        layout.setRowWeightY(0, 0.0);
        layout.setRowWeightY(1, 1.0);
        layout.setRowWeightX(1, 1.0);
        layout.setCellWeightX(0, 0, 1.0);
        layout.setCellWeightX(0, 1, 0.0);
        layout.setCellWeightX(0, 2, 0.0);

        table = new ProcessingParamsTable();
        final Component bandFilterButton = createBandFilterButton(table);
        final JLabel label = new JLabel("Bands to be included");

        components.add(bandFilterButton);
        components.add(label);

        final JPanel bandsPanel = new JPanel(layout);
        bandsPanel.add(layout.createHorizontalSpacer());
        bandsPanel.add(label);
        bandsPanel.add(bandFilterButton);
        bandsPanel.add(table.getComponent());

        return bandsPanel;
    }

    private Component createBandFilterButton(final ProcessingParamsTable table) {
        final AbstractButton bandFilterButton = ToolButtonFactory.createButton(UIUtils.loadImageIcon("icons/Copy16.gif"), false);
        bandFilterButton.setName("BandFilterButton");
        bandFilterButton.setToolTipText("Choose the bands to process");
        bandFilterButton.addActionListener(new BandFilterButtonListener(table));
        return bandFilterButton;
    }

    private String editExpression(String expression, final boolean booleanExpected) {
        final Product product;
        try {
            product = model.getSourceProducts()[0];
        } catch (IOException e) {
            return null;
        }
        if (product == null) {
            return null;
        }
        final ProductExpressionPane expressionPane;
        if (booleanExpected) {
            expressionPane = ProductExpressionPane.createBooleanExpressionPane(new Product[]{product}, product,
                                                                               appContext.getPreferences());
        } else {
            expressionPane = ProductExpressionPane.createGeneralExpressionPane(new Product[]{product}, product,
                                                                               appContext.getPreferences());
        }
        expressionPane.setCode(expression);
        final int i = expressionPane.showModalDialog(appContext.getApplicationWindow(), "Expression Editor");
        if (i == ModalDialog.ID_OK) {
            return expressionPane.getCode();
        }
        return null;
    }

    private static class Row {

        private final String bandName;
        private final String bitmaskExpression;
        private final String algorithmName;
        private final double weightCoefficient;
        private final double fillValue;

        public Row(String bandName, String bitmaskExpression, String algorithmName, double weightCoefficient, double fillValue) {
            this.bandName = bandName;
            this.bitmaskExpression = bitmaskExpression;
            this.algorithmName = algorithmName;
            this.weightCoefficient = weightCoefficient;
            this.fillValue = fillValue;
        }
    }

    private class ProcessingParamsTable {

        private JTable table;
        private DefaultTableModel tableModel;
        private final JScrollPane scrollPane;

        public ProcessingParamsTable() {
            tableModel = new DefaultTableModel() {
                @Override
                public boolean isCellEditable(int row, int column) {
                    return column != 0;
                }
            };

            tableModel.setColumnIdentifiers(new String[]{
                    "Band",
                    "Valid expression",
                    "Aggregation",
                    "Weight",
                    "Fill value"
            });

            tableModel.addTableModelListener(new MyTableModelListener());

            table = new JTable(tableModel) {
                @Override
                public Class getColumnClass(int column) {
                    if (column == 3) {
                        return Double.class;
                    } else {
                        return String.class;
                    }
                }

                @Override
                public void tableChanged(TableModelEvent e) {
                    super.tableChanged(e);
                }
            };
            table.getTableHeader().setReorderingAllowed(false);

            table.getColumnModel().getColumn(0).setMinWidth(60);
            table.getColumnModel().getColumn(1).setMinWidth(100);
            table.getColumnModel().getColumn(2).setMinWidth(60);
            table.getColumnModel().getColumn(3).setMinWidth(60);
            table.getColumnModel().getColumn(4).setMinWidth(60);

            table.getColumnModel().getColumn(1).setCellEditor(new ExpressionEditor(true));
            table.getColumnModel().getColumn(2).setCellEditor(new DefaultCellEditor(new JComboBox(aggregatorNames)));
            scrollPane = new JScrollPane(table);
            components.add(table);
            components.add(scrollPane);
        }

        public JComponent getComponent() {
            return scrollPane;
        }

        public String[] getBandNames() {
            final int numRows = table.getRowCount();
            final String[] bandNames = new String[numRows];
            for (int i = 0; i < bandNames.length; i++) {
                bandNames[i] = (String) table.getValueAt(i, 0);
            }
            return bandNames;
        }

        public void add(final String bandName, final String validExpression, String algorithmName,
                        double weightCoefficient, double fillValue) {
            if (algorithmName == null || !StringUtils.contains(aggregatorNames, algorithmName)) {
                algorithmName = AggregatorAverage.Descriptor.NAME;
            }
            tableModel.addRow(new Object[]{bandName, validExpression, algorithmName, weightCoefficient, fillValue});
        }

        public void remove(final String bandName) {
            final String[] bandNames = getBandNames();
            final int rowToRemove = StringUtils.indexOf(bandNames, bandName);
            tableModel.removeRow(rowToRemove);
        }

        public Row[] getRows() {
            final List dataList = tableModel.getDataVector();
            final Row[] rows = new Row[dataList.size()];
            for (int i = 0; i < dataList.size(); i++) {
                final List dataListRow = (List) dataList.get(i);
                rows[i] = new Row((String) dataListRow.get(0),
                                  (String) dataListRow.get(1),
                                  (String) dataListRow.get(2),
                                  (Double) dataListRow.get(3),
                                  (Double) dataListRow.get(4));
            }
            return rows;
        }

        public void clear() {
            final String[] bandNames = getBandNames();
            for (final String bandName : bandNames) {
                remove(bandName);
            }
            tableModel.setRowCount(0);
        }
    }

    private class MyTableModelListener implements TableModelListener {

        @Override
        public void tableChanged(TableModelEvent event) {
            try {
                final VariableConfig[] variableConfigs = new VariableConfig[table.getRows().length];
                final Row[] rows = table.getRows();
                for (int i = 0; i < rows.length; i++) {
                    final Row row = rows[i];
                    final VariableConfig config = new VariableConfig();
                    config.name = row.bandName;
                    config.expression = row.bitmaskExpression;
                    config.aggregator = AggregatorDescriptorRegistry.getInstance().getAggregatorDescriptor(row.algorithmName);
                    config.weight = row.weightCoefficient;
                    config.fillValue = row.fillValue;
                    variableConfigs[i] = config;
                }
                model.setProperty(BinningModel.PROPERTY_KEY_VARIABLE_CONFIGS, variableConfigs);
            } catch (ValidationException e1) {
                appContext.handleError("Unable to validate variable configurations.", e1);
            }
        }
    }

    private class ExpressionEditor extends AbstractCellEditor implements TableCellEditor {

        private final JPanel editorComponent;
        private final JTextField textField;

        public ExpressionEditor(final boolean booleanExpected) {

            JButton button = new JButton("...");
            final Dimension preferredSize = button.getPreferredSize();
            preferredSize.setSize(25, preferredSize.getHeight());
            button.setPreferredSize(preferredSize);

            final ActionListener actionListener = new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    final String expression = editExpression(textField.getText(), booleanExpected);
                    if (expression != null) {
                        textField.setText(expression);
                        fireEditingStopped();
                    } else {
                        fireEditingCanceled();
                    }
                }
            };
            button.addActionListener(actionListener);

            textField = new JTextField();

            editorComponent = new JPanel(new BorderLayout());
            editorComponent.add(textField);
            editorComponent.add(button, BorderLayout.EAST);
        }

        public Object getCellEditorValue() {
            return textField.getText();
        }

        public Component getTableCellEditorComponent(JTable table,
                                                     Object value,
                                                     boolean isSelected,
                                                     int row,
                                                     int column) {
            textField.setText((String) value);
            return editorComponent;
        }
    }

    private class BandFilterButtonListener implements ActionListener {

        private final ProcessingParamsTable table;

        public BandFilterButtonListener(ProcessingParamsTable table) {
            this.table = table;
        }

        @Override
        public void actionPerformed(ActionEvent event) {
            Product exampleProduct = null;
            try {
                exampleProduct = model.getSourceProducts()[0];
            } catch (IOException e) {
                appContext.handleError("Unable to select the bands which are to process.", e);
            }
            if (exampleProduct != null) {
                final Band[] allBands = exampleProduct.getBands();
                final String[] existingBandNames = table.getBandNames();
                final List<Band> existingBandList = new ArrayList<Band>();
                for (String existingBandName : existingBandNames) {
                    final Band band = exampleProduct.getBand(existingBandName);
                    if (band != null) {
                        existingBandList.add(band);
                    }
                }
                final Band[] existingBands = existingBandList.toArray(
                        new Band[existingBandList.size()]);
                final BandChooser bandChooser = new BandChooser(appContext.getApplicationWindow(),
                                                                "Band Chooser", "", /*I18N*/
                                                                allBands,
                                                                existingBands);
                if (bandChooser.show() == ModalDialog.ID_OK) {
                    final Row[] rows = table.getRows();
                    final HashMap<String, Row> rowsMap = new HashMap<String, Row>();
                    for (final Row row : rows) {
                        rowsMap.put(row.bandName, row);
                    }
                    table.clear();

                    final Band[] selectedBands = bandChooser.getSelectedBands();

                    for (final Band selectedBand : selectedBands) {
                        final String bandName = selectedBand.getName();
                        if (rowsMap.containsKey(bandName)) {
                            final Row row = rowsMap.get(bandName);
                            table.add(bandName, row.bitmaskExpression, row.algorithmName,
                                      row.weightCoefficient, row.fillValue);
                        } else {
                            table.add(bandName, selectedBand.getValidMaskExpression(), AggregatorAverage.Descriptor.NAME, 1.0, selectedBand.getNoDataValue());
                        }
                    }
                }
            }
        }
    }

    public static class VariableConfig {

        public String name;
        public String expression;
        public AggregatorDescriptor aggregator;
        public Double weight;
        public Double fillValue;
    }

}
