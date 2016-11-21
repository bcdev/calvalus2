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
import com.bc.calvalus.portal.shared.DtoValueRange;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HTMLPanel;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Arrays;
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

        boolean onOk();
    }

    private final Map<DtoParameterDescriptor, ParameterEditor> editorMap;

    private final String title;
    private final DtoParameterDescriptor[] parameterDescriptors;
    private final CalvalusStyle style;

    public ParametersEditorGenerator(String title, DtoParameterDescriptor[] parameterDescriptors, CalvalusStyle style) {
        this.title = title;
        this.parameterDescriptors = parameterDescriptors;
        this.style = style;
        this.editorMap = new HashMap<DtoParameterDescriptor, ParameterEditor>();
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            editorMap.put(parameterDescriptor, createEditor(parameterDescriptor));
        }
    }

    public void showDialog(String width, String height, String description, final OnOkHandler onOkHandler) {
        ScrollPanel scrollPanel = createParameterPanel(width, height);
        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.add(scrollPanel);
        verticalPanel.add(new HTMLPanel(description));
        showDialog(onOkHandler, verticalPanel);

    }

    public void showDialog(String width, String height, final OnOkHandler onOkHandler) {
        ScrollPanel scrollPanel = createParameterPanel(width, height);
        showDialog(onOkHandler, scrollPanel);
    }

    private ScrollPanel createParameterPanel(String width, String height) {
        FlexTable tableWidget = createTableWidget();

        ScrollPanel scrollPanel = new ScrollPanel(tableWidget);
        scrollPanel.setWidth(width);
        scrollPanel.setHeight(height);
        return scrollPanel;
    }

    private void showDialog(final OnOkHandler onOkHandler, final Widget widget) {
        final Dialog dialog = new Dialog(title, widget, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onOk() {
                if (onOkHandler.onOk()) {
                    hide();
                }
            }
        };
        dialog.show();
    }

    public void setAvailableVariables(List<String> variableNames) {
        String[] variableNameArray = variableNames.toArray(new String[variableNames.size()]);
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            if (parameterDescriptor.getType().startsWith("variable")) {
                ParameterEditor parameterEditor = editorMap.get(parameterDescriptor);
                if (parameterEditor instanceof SelectParameterEditor) {
                    SelectParameterEditor selectParameterEditor = (SelectParameterEditor) parameterEditor;
                    selectParameterEditor.updateValueSet(variableNameArray);
                }
            }
        }
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

    public String getParameterValue(DtoParameterDescriptor parameterDescriptor) throws ValidationException {
        ParameterEditor parameterEditor = editorMap.get(parameterDescriptor);
        if (parameterEditor != null) {
            return parameterEditor.getValue();
        }
        return null;
    }

    public void setParameterValue(DtoParameterDescriptor parameterDescriptor, String value) {
        ParameterEditor parameterEditor = editorMap.get(parameterDescriptor);
        if (parameterEditor != null) {
            parameterEditor.setValue(value);
        }
    }


    public String formatAsXMLFromWidgets() throws ValidationException {
        StringBuilder sb = new StringBuilder();
        sb.append("<parameters>\n");
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            String value = editorMap.get(parameterDescriptor).getValue();
            formatParameterAsXML(sb, parameterDescriptor.getName(), value);
        }
        sb.append("</parameters>\n");
        return sb.toString();
    }

    public void setFromXML(String xml) {
        int index = xml.indexOf("<parameters>");
        if (index != -1) {
            // read past <parameters>
            final int paramStart = index + "<parameters>".length();
            for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
                String name = parameterDescriptor.getName();
                int tagStartIndex = xml.indexOf("<" + name + ">", paramStart);
                String value = parameterDescriptor.getDefaultValue();
                if (tagStartIndex != -1) {
                    int valueStartIndex = tagStartIndex + name.length() + 2;
                    int endIndex = xml.indexOf("</" + name + ">", valueStartIndex);
                    if (endIndex != -1) {
                        String encodedValue = xml.substring(valueStartIndex, endIndex);
                        value = decodeXML(encodedValue);
                    }
                }
                editorMap.get(parameterDescriptor).setValue(value);
            }
        }
    }

    static String formatAsXMLFromDefaults(DtoParameterDescriptor[] parameterDescriptors) {
        StringBuilder sb = new StringBuilder();
        sb.append("<parameters>\n");
        for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
            String defaultValue = parameterDescriptor.getDefaultValue();
            formatParameterAsXML(sb, parameterDescriptor.getName(), defaultValue);
        }
        sb.append("</parameters>\n");
        return sb.toString();
    }

    private static void formatParameterAsXML(StringBuilder sb, String name, String value) {
        if (value != null) {
            sb.append("  <");
            sb.append(name);
            sb.append(">");
            sb.append(encodeXML(value));
            sb.append("</");
            sb.append(name);
            sb.append(">\n");
        }
    }

    private static String encodeXML(String s) {
        return s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    }

    private static String decodeXML(String s) {
        return s.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&");
    }

    private static String[] decodeXMLArray(String[] valueSet) {
        String[] decodedValueSet = new String[valueSet.length];
        for (int i = 0; i < valueSet.length; i++) {
            decodedValueSet[i] = decodeXML(valueSet[i]);
        }
        return decodedValueSet;
    }

    private static ParameterEditor createEditor(DtoParameterDescriptor parameterDescriptor) {
        String paramName = parameterDescriptor.getName();
        String type = parameterDescriptor.getType();
        String defaultValue = parameterDescriptor.getDefaultValue();

        ParameterEditor editor = null;
        if (type.equalsIgnoreCase("boolean")) {
            editor = new BooleanParameterEditor(defaultValue);
        } else if (type.equalsIgnoreCase("string")) {
            String decodedValue = decodeXML(defaultValue);
            String[] valueSet = parameterDescriptor.getValueSet();
            if (valueSet.length > 0) {
                String[] decodedValueSet = decodeXMLArray(valueSet);
                SelectParameterEditor selectParameterEditor = new SelectParameterEditor(decodedValue, decodedValueSet, false);
                selectParameterEditor.updateValueSet(decodedValueSet);
                editor = selectParameterEditor;
            } else {
                editor = new TextParameterEditor(decodedValue);
            }
        } else if (type.equalsIgnoreCase("stringArray")) {
            String[] valueSet = parameterDescriptor.getValueSet();
            editor = new SelectParameterEditor(defaultValue, decodeXMLArray(valueSet), true);
        } else if (type.equalsIgnoreCase("float")) {
            DtoValueRange valueRange = parameterDescriptor.getValueRange();
            editor = new FloatParameterEditor(paramName, defaultValue, valueRange);
        } else if (type.equalsIgnoreCase("int")) {
            DtoValueRange valueRange = parameterDescriptor.getValueRange();
            editor = new IntParameterEditor(paramName, defaultValue, valueRange);
        } else if (type.equalsIgnoreCase("variable")) {
            editor = new SelectParameterEditor(defaultValue, new String[0], false);
        } else if (type.equalsIgnoreCase("variableArray")) {
            editor = new SelectParameterEditor(defaultValue, new String[0], true);
        }
        if (editor == null) {
            // fallback
            editor = new TextParameterEditor(decodeXML(defaultValue));
        }
        return editor;
    }

    interface ParameterEditor {
        String getValue() throws ValidationException;

        void setValue(String value);

        Widget getWidget();
    }

    private static class BooleanParameterEditor implements ParameterEditor {

        private final CheckBox checkBox;

        public BooleanParameterEditor(String defaultValue) {
            checkBox = new CheckBox();
            checkBox.setValue(Boolean.valueOf(defaultValue));
        }

        @Override
        public String getValue() throws ValidationException {
            return checkBox.getValue().toString();
        }

        @Override
        public void setValue(String value) {
            checkBox.setValue(Boolean.valueOf(value));
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
        public String getValue() throws ValidationException {
            return textBox.getValue().trim();
        }

        @Override
        public void setValue(String value) {
            textBox.setValue(value);
        }

        @Override
        public Widget getWidget() {
            return textBox;
        }
    }

    private static class FloatParameterEditor extends TextParameterEditor {

        private final String paramName;
        private final DtoValueRange valueRange;

        public FloatParameterEditor(String paramName, String defaultValue, DtoValueRange valueRange) {
            super(defaultValue);
            this.paramName = paramName;
            this.valueRange = valueRange;
        }

        @Override
        public String getValue() throws ValidationException {
            String textValue = super.getValue();
            try {
                double doubleValue = Double.parseDouble(textValue);
                if (valueRange != null && !valueRange.contains(doubleValue)) {
                    String msg = "Value for '" + paramName + "' is out of range " + valueRange.toString() + ".";
                    throw new ValidationException(getWidget(), msg);
                }
            } catch (NumberFormatException nfe) {
                String msg = "The value for '" + paramName + "'is not a floating point value.";
                throw new ValidationException(getWidget(), msg);
            }
            return textValue;
        }
    }

    private static class IntParameterEditor extends TextParameterEditor {

        private final String paramName;
        private final DtoValueRange valueRange;

        public IntParameterEditor(String paramName, String defaultValue, DtoValueRange valueRange) {
            super(defaultValue);
            this.paramName = paramName;
            this.valueRange = valueRange;
        }

        @Override
        public String getValue() throws ValidationException {
            String textValue = super.getValue();
            try {
                long longValue = Long.parseLong(textValue);
                if (valueRange != null && !valueRange.contains(longValue)) {
                    String msg = "Value for '" + paramName + "' is out of range " + valueRange.toString() + ".";
                    throw new ValidationException(getWidget(), msg);
                }
            } catch (NumberFormatException nfe) {
                String msg = "The value for '" + paramName + "'is not an integer value.";
                throw new ValidationException(getWidget(), msg);
            }
            return textValue;
        }
    }

    private static class SelectParameterEditor implements ParameterEditor {

        private final ListBox listBox;

        public SelectParameterEditor(String defaultValue, String[] valueSet, boolean multiSelect) {
            List<String> defaultValues = new ArrayList<String>();
            if (!defaultValue.isEmpty()) {
                if (multiSelect && defaultValue.contains(",")) {
                    for (String s : defaultValue.split("\\,")) {
                        defaultValues.add(s.trim());
                    }
                } else {
                    defaultValues.add(defaultValue);
                }
            }
            listBox = new ListBox();
            listBox.setMultipleSelect(multiSelect);
            fillListbox(valueSet, defaultValues);
        }

        @Override
        public String getValue() throws ValidationException {
            StringBuilder sb = new StringBuilder();
            int itemCount = listBox.getItemCount();
            for (int i = 0; i < itemCount; i++) {
                if (listBox.isItemSelected(i)) {
                    sb.append(listBox.getValue(i));
                    sb.append(",");
                }
            }
            if (sb.length() > 1) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }

        @Override
        public void setValue(String value) {
            List valueItems = Arrays.asList(value.split(","));
            for (int i = 0; i < listBox.getItemCount(); i++) {
                boolean selected = valueItems.contains(listBox.getValue(i));
                listBox.setItemSelected(i, selected);
            }
        }

        @Override
        public Widget getWidget() {
            return listBox;
        }

        public void updateValueSet(String[] valueSet) {
            List<String> selected = getSelected();
            listBox.clear();
            fillListbox(valueSet, selected);
        }

        private void fillListbox(String[] allItems, List<String> selectedItems) {
            for (int i = 0; i < allItems.length; i++) {
                listBox.addItem(allItems[i]);
                if (selectedItems.contains(allItems[i])) {
                    listBox.setItemSelected(i, true);
                }
            }
        }

        private List<String> getSelected() {
            List<String> selected = new ArrayList<String>();
            int itemCount = listBox.getItemCount();
            for (int i = 0; i < itemCount; i++) {
                if (listBox.isItemSelected(i)) {
                    selected.add(listBox.getValue(i));
                }
            }
            return selected;
        }
    }
}
