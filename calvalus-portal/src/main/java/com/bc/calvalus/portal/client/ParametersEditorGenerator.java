/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.portal.shared.DtoParameterDescriptor;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For generating a dialog based on {@link com.bc.calvalus.portal.shared.DtoParameterDescriptor}s
 *
 * @author marcoz
 */
public class ParametersEditorGenerator {

    interface OnOkHandler {
        void onOk();
    }

    private final Map<DtoParameterDescriptor, ParameterEditor> editorMap;
    private final String title;
    private final DtoParameterDescriptor[] parameterDescriptors;
    private final L2ConfigForm.L2Style style;

    public ParametersEditorGenerator(String title, DtoParameterDescriptor[] parameterDescriptors, L2ConfigForm.L2Style style) {
        this.title = title;
        this.parameterDescriptors = parameterDescriptors;
        this.style = style;
        editorMap = new HashMap<DtoParameterDescriptor, ParameterEditor>();
    }

    public void showDialog(final OnOkHandler onOkHandler) {
        ensureParameterWidgetsCreated();
        FlexTable tableWidget = createTableWidget();

        ScrollPanel scrollPanel = new ScrollPanel(tableWidget);
        scrollPanel.setWidth("800px");
        scrollPanel.setHeight("640px");

        final Dialog dialog = new Dialog(title,
                                         scrollPanel,
                                         Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onOk() {
                onOkHandler.onOk();
                hide();
            }
        };
        dialog.show();
    }

    private FlexTable createTableWidget() {
        FlexTable paramTable = new FlexTable();
        FlexTable.FlexCellFormatter flexCellFormatter = paramTable.getFlexCellFormatter();
        paramTable.setCellSpacing(3);
        int row = 0;
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            paramTable.setWidget(row, 0, new HTML(parameterDescriptor.getName() + ":"));
            flexCellFormatter.setVerticalAlignment(row, 0, HasVerticalAlignment.ALIGN_TOP);
            flexCellFormatter.setHorizontalAlignment(row, 0, HasHorizontalAlignment.ALIGN_LEFT);
            paramTable.setWidget(row, 1, editorMap.get(parameterDescriptor).getWidget());
            flexCellFormatter.setVerticalAlignment(row, 1, HasVerticalAlignment.ALIGN_TOP);
            flexCellFormatter.setHorizontalAlignment(row, 1, HasHorizontalAlignment.ALIGN_LEFT);
            String description = parameterDescriptor.getDescription();
            if (description != null && !description.isEmpty()) {
                paramTable.setWidget(row, 2, new HTML(description));
                flexCellFormatter.addStyleName(row, 2, style.explanatoryLabel());
                flexCellFormatter.setVerticalAlignment(row, 2, HasVerticalAlignment.ALIGN_TOP);
                flexCellFormatter.setHorizontalAlignment(row, 2, HasHorizontalAlignment.ALIGN_LEFT);
            }
            row++;
        }
        paramTable.getColumnFormatter().setWidth(0, "20%");
        paramTable.getColumnFormatter().setWidth(1, "30%");
        paramTable.getColumnFormatter().setWidth(2, "50%");
        return paramTable;
    }

    public String formatAsXMLFromWidgets() {
        StringBuilder sb = new StringBuilder();
        sb.append("<parameters>\n");
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            sb.append("  <");
            sb.append(parameterDescriptor.getName());
            sb.append(">");
            sb.append(editorMap.get(parameterDescriptor).getValue());
            sb.append("</");
            sb.append(parameterDescriptor.getName());
            sb.append(">\n");
        }

        sb.append("</parameters>\n");
        return sb.toString();
    }

    private void ensureParameterWidgetsCreated() {
        if (!editorMap.isEmpty()) {
            return;
        }
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            editorMap.put(parameterDescriptor, createEditor(parameterDescriptor));
        }
    }

    private static ParameterEditor createEditor(DtoParameterDescriptor parameterDescriptor) {
        String type = parameterDescriptor.getType();
        String defaultValue = parameterDescriptor.getDefaultValue();

        ParameterEditor editor = null;
        if (type.equalsIgnoreCase("boolean")) {
            editor = new BooleanParameterEditor(defaultValue);
        } else if (type.equalsIgnoreCase("string")) {
            String[] valueSet = parameterDescriptor.getValueSet();
            if (valueSet.length > 0) {
                editor = new SelectParameterEditor(defaultValue, valueSet, false);
            } else {
                editor = new TextParameterEditor(defaultValue);
            }
        } else if (type.equalsIgnoreCase("stringArray")) {
            String[] valueSet = parameterDescriptor.getValueSet();
            editor = new SelectParameterEditor(defaultValue, valueSet, true);
        }
        return editor;
    }

    interface ParameterEditor {
        String getValue();

        Widget getWidget();
    }

    private static class BooleanParameterEditor implements ParameterEditor {

        private final CheckBox checkBox;

        public BooleanParameterEditor(String defaultValue) {
            checkBox = new CheckBox();
            checkBox.setValue(Boolean.valueOf(defaultValue));
        }

        @Override
        public String getValue() {
            return checkBox.getValue().toString();
        }

        @Override
        public Widget getWidget() {
            return checkBox;
        }
    }

    private static class TextParameterEditor implements ParameterEditor {

        private final TextBox textBox;

        public TextParameterEditor(String defaultValue) {
            textBox = new TextBox();
            if (defaultValue != null) {
                textBox.setValue(defaultValue);
                if (textBox.getVisibleLength() < defaultValue.length()) {
                    textBox.setVisibleLength(36);
                }
            }
        }

        @Override
        public String getValue() {
            return textBox.getValue();
        }

        @Override
        public Widget getWidget() {
            return textBox;
        }
    }

    private static class SelectParameterEditor implements ParameterEditor {

        private final ListBox listBox;

        public SelectParameterEditor(String defaultValue, String[] valueSet, boolean multiSelect) {
            List<String> defaultValues = new ArrayList<String>();
            if (multiSelect && defaultValue.contains(",")) {
                for (String s : defaultValue.split("\\,")) {
                    defaultValues.add(s.trim());
                }
            } else {
                defaultValues.add(defaultValue);
            }
            listBox = new ListBox(multiSelect);
            for (int j = 0; j < valueSet.length; j++) {
                String value = valueSet[j];
                listBox.addItem(value);
                if (defaultValues.contains(value)) {
                    listBox.setItemSelected(j, true);
                }
            }
        }

        @Override
        public String getValue() {
            StringBuilder sb = new StringBuilder();
            int itemCount = listBox.getItemCount();
            for (int i = 0; i < itemCount; i++) {
                if (listBox.isItemSelected(i)) {
                    sb.append(listBox.getValue(i));
                    sb.append(",");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        @Override
        public Widget getWidget() {
            return listBox;
        }
    }
}
