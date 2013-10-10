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

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.shared.DtoParameterDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.production.hadoop.ProcessorProductionRequest;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.resources.client.CssResource;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L2ConfigForm extends Composite {

    public static final String NO_PROCESSOR_SELCTION = "<none>";

    interface TheUiBinder extends UiBinder<Widget, L2ConfigForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    interface L2Style extends CssResource {

        String explanatoryValue();

        String explanatoryLabel();

        String centeredHorizontalPanel();
    }

    @UiField
    L2Style style;


    @UiField
    ListBox processorList;
    @UiField
    Label processorBundleName;

    @UiField
    HTML processorDescriptionHTML;

    @UiField
    TextArea processorParametersArea;
    @UiField
    FileUpload fileUpload;
    @UiField
    FormPanel uploadForm;
    @UiField
    Button editParametersButton;
    @UiField
    CheckBox showMyProcessors;
    @UiField
    CheckBox showSystemProcessors;
    @UiField
    CheckBox showAllUserProcessors;

    private final boolean selectionMandatory;
    private final PortalContext portalContext;
    private final Filter<DtoProcessorDescriptor> processorFilter;
    private final Map<DtoParameterDescriptor, Widget> parameterDescriptorWidgets;
    private final List<DtoProcessorDescriptor> processorDescriptors;

    public L2ConfigForm(PortalContext portalContext, boolean selectionMandatory) {
        this(portalContext, null, selectionMandatory);
    }

    public L2ConfigForm(PortalContext portalContext, Filter<DtoProcessorDescriptor> processorFilter, boolean selectionMandatory) {
        this.portalContext = portalContext;
        this.processorFilter = processorFilter;
        this.selectionMandatory = selectionMandatory;
        this.processorDescriptors = new ArrayList<DtoProcessorDescriptor>();
        initWidget(uiBinder.createAndBindUi(this));

        FileUploadManager.submitOnChange(uploadForm, fileUpload, "echo=xml",
                                         new FormPanel.SubmitHandler() {
                                             @Override
                                             public void onSubmit(FormPanel.SubmitEvent event) {
                                                 // we can check for valid input here
                                             }
                                         },
                                         new FormPanel.SubmitCompleteHandler() {
                                             @Override
                                             public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                                                 String results = event.getResults();
                                                 String text = "";
                                                 if (results != null) {
                                                     text = FileUploadManager.decodeXML(results);
                                                 }
                                                 processorParametersArea.setText(text);
                                             }
                                         }
        );
        parameterDescriptorWidgets = new HashMap<DtoParameterDescriptor, Widget>();
        editParametersButton.addClickHandler(new EditParametersAction());

        updateProcessorList();
        processorList.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateProcessorDetails();
            }
        });

        ValueChangeHandler<Boolean> valueChangeHandler = new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                updateProcessorList();
            }
        };
        showMyProcessors.addValueChangeHandler(valueChangeHandler);
        showAllUserProcessors.addValueChangeHandler(valueChangeHandler);
        showSystemProcessors.addValueChangeHandler(valueChangeHandler);

        updateProcessorDetails();

    }

    public void updateProcessorList() {
        DtoProcessorDescriptor oldSelection = getSelectedProcessorDescriptor();

        processorDescriptors.clear();
        if (showSystemProcessors.getValue()) {
            Collections.addAll(processorDescriptors, portalContext.getProcessors(BundleFilter.PROVIDER_SYSTEM));
        }
        if (showMyProcessors.getValue()) {
            Collections.addAll(processorDescriptors, portalContext.getProcessors(BundleFilter.PROVIDER_USER));
        }
        if (showAllUserProcessors.getValue()) {
            Collections.addAll(processorDescriptors, portalContext.getProcessors(BundleFilter.PROVIDER_ALL_USERS));
        }

        if (processorFilter != null) {
            final Iterator<DtoProcessorDescriptor> iterator = processorDescriptors.iterator();
            while (iterator.hasNext()) {
                DtoProcessorDescriptor productSet = iterator.next();
                if (!processorFilter.accept(productSet)) {
                    iterator.remove();
                }
            }
        }

        processorList.clear();
        if (!selectionMandatory) {
            processorList.addItem(NO_PROCESSOR_SELCTION);
        }
        int newSelectionIndex = 0;
        boolean productSetChanged = true;
        for (DtoProcessorDescriptor processor : processorDescriptors) {
            String label = processor.getProcessorName() + " v" + processor.getProcessorVersion();
            processorList.addItem(label);
            if (oldSelection != null && oldSelection.equals(processor)) {
                newSelectionIndex = processorList.getItemCount() - 1;
                productSetChanged = false;
            }
        }
        processorList.setSelectedIndex(selectionMandatory ? newSelectionIndex : newSelectionIndex + 1);
        if (productSetChanged) {
            DomEvent.fireNativeEvent(Document.get().createChangeEvent(), processorList);
        }
    }

    private void updateProcessorDetails() {
        DtoProcessorDescriptor processorDescriptor = getSelectedProcessorDescriptor();
        if (processorDescriptor != null) {
            processorBundleName.setText("Bundle: " + processorDescriptor.getBundleName() + " v" + processorDescriptor.getBundleVersion());
            processorParametersArea.setValue(processorDescriptor.getDefaultParameter());
            processorDescriptionHTML.setHTML(processorDescriptor.getDescriptionHtml());
            editParametersButton.setEnabled(processorDescriptor.getParameterDescriptors().length > 0);
        } else {
            processorBundleName.setText("");
            processorParametersArea.setValue("");
            processorDescriptionHTML.setHTML("");
        }
        parameterDescriptorWidgets.clear();
    }

    public HandlerRegistration addChangeHandler(final ChangeHandler changeHandler) {
        return processorList.addChangeHandler(changeHandler);
    }

    public DtoProcessorDescriptor getSelectedProcessorDescriptor() {
        int selectedIndex = processorList.getSelectedIndex();
        int offset = selectionMandatory ? 0 : 1;
        if (selectedIndex >= offset) {
            return processorDescriptors.get(selectedIndex - offset);
        } else {
            return null;
        }
    }

    public String getProcessorParameters() {
        return processorParametersArea.getValue();
    }

    public void validateForm() throws ValidationException {
        DtoProcessorDescriptor processorDescriptor = getSelectedProcessorDescriptor();
        boolean processorDescriptorValid = !selectionMandatory || processorDescriptor != null;
        if (!processorDescriptorValid) {
            throw new ValidationException(processorList, "No processor selected.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        DtoProcessorDescriptor processorDescriptor = getSelectedProcessorDescriptor();
        if (processorDescriptor != null) {
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, processorDescriptor.getBundleName());
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, processorDescriptor.getBundleVersion());
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, processorDescriptor.getBundleLocation());
            parameters.put(ProcessorProductionRequest.PROCESSOR_NAME, processorDescriptor.getExecutableName());
            parameters.put(ProcessorProductionRequest.PROCESSOR_PARAMETERS, getProcessorParameters());
        }
        return parameters;
    }

    private class EditParametersAction implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            DtoProcessorDescriptor processor = getSelectedProcessorDescriptor();
            final DtoParameterDescriptor[] parameterDescriptors = processor.getParameterDescriptors();
            ensureParameterWidgetsCreated(parameterDescriptors);
            FlexTable tableWidget = createTableWidget(parameterDescriptors);

            ScrollPanel scrollPanel = new ScrollPanel(tableWidget);
            scrollPanel.setWidth("800px");
            scrollPanel.setHeight("640px");

            String title = "Edit Parameters for " + processor.getProcessorName() + " v" + processor.getProcessorVersion();
            final Dialog dialog = new Dialog(title,
                                             scrollPanel,
                                             Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
                @Override
                protected void onOk() {
                    processorParametersArea.setValue(formatAsXML(parameterDescriptors));
                    hide();
                }
            };
            dialog.show();
        }

        private FlexTable createTableWidget(DtoParameterDescriptor[] parameterDescriptors) {
            FlexTable paramTable = new FlexTable();
            FlexTable.FlexCellFormatter flexCellFormatter = paramTable.getFlexCellFormatter();
            paramTable.setCellSpacing(3);
            int row = 0;
            for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
                paramTable.setWidget(row, 0, new HTML(parameterDescriptor.getName() + ":"));
                flexCellFormatter.setVerticalAlignment(row, 0, HasVerticalAlignment.ALIGN_TOP);
                flexCellFormatter.setHorizontalAlignment(row, 0, HasHorizontalAlignment.ALIGN_LEFT);
                paramTable.setWidget(row, 1, parameterDescriptorWidgets.get(parameterDescriptor));
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

        private String formatAsXML(DtoParameterDescriptor[] parameterDescriptors) {
            StringBuilder sb = new StringBuilder();
            sb.append("<parameters>\n");
            for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
                Widget widget = parameterDescriptorWidgets.get(parameterDescriptor);
                sb.append("  <");
                sb.append(parameterDescriptor.getName());
                sb.append(">");
                if (widget instanceof CheckBox) {
                    CheckBox checkBox = (CheckBox) widget;
                    sb.append(checkBox.getValue().toString());
                } else if (widget instanceof TextBox) {
                    TextBox textBox = (TextBox) widget;
                    sb.append(textBox.getValue());
                } else if (widget instanceof ListBox) {
                    ListBox listBox = (ListBox) widget;
                    int itemCount = listBox.getItemCount();
                    for (int i = 0; i < itemCount; i++) {
                        if (listBox.isItemSelected(i)) {
                            sb.append(listBox.getValue(i));
                            sb.append(",");
                        }
                    }
                    sb.deleteCharAt(sb.length() - 1);
                }
                sb.append("</");
                sb.append(parameterDescriptor.getName());
                sb.append(">\n");
            }

            sb.append("</parameters>\n");
            return sb.toString();
        }

        private void ensureParameterWidgetsCreated(DtoParameterDescriptor[] parameterDescriptors) {
            if (!parameterDescriptorWidgets.isEmpty()) {
                return;
            }
            for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
                String type = parameterDescriptor.getType();
                String defaultValue = parameterDescriptor.getDefaultValue();

                Widget widget = null;
                if (type.equalsIgnoreCase("boolean")) {
                    widget = createCheckBox(defaultValue);
                } else if (type.equalsIgnoreCase("string")) {
                    String[] valueSet = parameterDescriptor.getValueSet();
                    if (valueSet.length > 0) {
                        widget = createListBox(defaultValue, valueSet, false);
                    } else {
                        widget = createTextBox(defaultValue);
                    }
                } else if (type.equalsIgnoreCase("stringArray")) {
                    String[] valueSet = parameterDescriptor.getValueSet();
                    widget = createListBox(defaultValue, valueSet, true);
                }
                parameterDescriptorWidgets.put(parameterDescriptor, widget);
            }
        }

        private ListBox createListBox(String defaultValue, String[] valueSet, boolean multipleSelect) {
            List<String> defaultValues = new ArrayList<String>();
            if (multipleSelect && defaultValue.contains(",")) {
                for (String s : defaultValue.split("\\,")) {
                    defaultValues.add(s.trim());
                }
            } else {
                defaultValues.add(defaultValue);
            }
            ListBox listBox = new ListBox(multipleSelect);
            for (int j = 0; j < valueSet.length; j++) {
                String value = valueSet[j];
                listBox.addItem(value);
                if (defaultValues.contains(value)) {
                    listBox.setItemSelected(j, true);
                }
            }
            return listBox;
        }

        private TextBox createTextBox(String defaultValue) {
            TextBox textBox = new TextBox();
            if (defaultValue != null) {
                textBox.setValue(defaultValue);
                if (textBox.getVisibleLength() < defaultValue.length()) {
                    textBox.setVisibleLength(36);
                }
            }
            return textBox;
        }

        private CheckBox createCheckBox(String defaultValue) {
            CheckBox checkBox = new CheckBox();
            checkBox.setValue(Boolean.valueOf(defaultValue));
            return checkBox;
        }
    }
}