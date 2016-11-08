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
import com.bc.calvalus.portal.shared.DtoProductSet;
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
import com.google.web.bindery.event.shared.HandlerRegistration;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextArea;
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

    public static final String NO_PROCESSOR_SELECTION = "<none>";

    interface TheUiBinder extends UiBinder<Widget, L2ConfigForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    CalvalusStyle style;


    @UiField
    Label processorListLabel;
    @UiField
    ListBox processorList;
    @UiField
    HTML processorBundleName;

    @UiField
    HTML processorDescriptionHTML;

    @UiField
    Label parametersLabel;
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
    @UiField
    CheckBox filterProcessorByVersion;
    @UiField
    CheckBox filterProcessorByProductType;
    @UiField
    Anchor showProcessorSelectionHelp;

    private final boolean selectionMandatory;
    private final PortalContext portalContext;
    private final Filter<DtoProcessorDescriptor> processorFilter;
    private final List<DtoProcessorDescriptor> processorDescriptors;

    private HandlerRegistration editParamsHandlerRegistration;
    private DtoProductSet productSet;

    public L2ConfigForm(PortalContext portalContext, boolean selectionMandatory) {
        this(portalContext, new L2ProcessorFilter(), selectionMandatory);
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
        filterProcessorByVersion.addValueChangeHandler(valueChangeHandler);
        filterProcessorByProductType.addValueChangeHandler(valueChangeHandler);
        filterProcessorByProductType.setEnabled(false);

        showAllUserProcessors.setEnabled(portalContext.withPortalFeature("otherSets"));

        updateProcessorDetails();

        HelpSystem.addClickHandler(showProcessorSelectionHelp, "processorSelection");
    }

    public void setProductSet(DtoProductSet productSet) {
        this.productSet = productSet;
        filterProcessorByProductType.setEnabled(productSet != null);
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

        final Iterator<DtoProcessorDescriptor> iterator = processorDescriptors.iterator();
        while (iterator.hasNext()) {
            DtoProcessorDescriptor processorDescriptor = iterator.next();
            if (BundleFilter.DUMMY_PROCESSOR_NAME.equals(processorDescriptor.getProcessorName())
                || (processorFilter != null && !processorFilter.accept(processorDescriptor))
                || (filterProcessorByVersion.getValue() && !isNewestVersion(processorDescriptor, processorDescriptors))
                || (filterProcessorByProductType.getValue() && !isMatchingInput(productSet, processorDescriptor))) {
                iterator.remove();
            }
        }

        processorList.clear();
        if (!selectionMandatory) {
            processorList.addItem(NO_PROCESSOR_SELECTION);
        }
        int newSelectionIndex = 0;
        boolean productSetChanged = true;
        for (DtoProcessorDescriptor processor : processorDescriptors) {
            processorList.addItem(processor.getDisplayText());
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

    private static boolean isMatchingInput(DtoProductSet productSet, DtoProcessorDescriptor processor) {
        if (productSet == null) {
            return true;
        }
        if (processor.getInputProductTypes() == null) {
            return false;
        }
        for (String type : processor.getInputProductTypes()) {
            if ("*".equals(type) || type.equals(productSet.getProductType())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNewestVersion(DtoProcessorDescriptor processor, List<DtoProcessorDescriptor> processorDescriptors) {
        for (DtoProcessorDescriptor other : processorDescriptors) {
            if (other.getProcessorName().equals(processor.getProcessorName()) &&
                other.getProcessorVersion().compareTo(processor.getProcessorVersion()) > 0) {
                return false;
            }
        }
        return true;
    }

    private void updateProcessorDetails() {
        if (editParamsHandlerRegistration != null) {
            editParamsHandlerRegistration.removeHandler();
        }
        DtoProcessorDescriptor processor = getSelectedProcessorDescriptor();
        if (processor != null) {
            String owner = "System";
            if (!processor.getOwner().isEmpty()) {
                owner = processor.getOwner();
            }
            StringBuilder types = new StringBuilder();
            if (processor.getInputProductTypes() != null) {
                for (String type : processor.getInputProductTypes()) {
                    if (types.length() != 0) {
                        types.append(", ");
                    }
                    types.append(type);
                }
            }
            String text = "Input Types: " + types.toString();
            text += "<br>Bundle: " + processor.getBundleName() + " v" + processor.getBundleVersion();
            text += "<br>Owner: " + owner;
            processorBundleName.setHTML(text);

            String defaultParameter = processor.getDefaultParameter();
            DtoParameterDescriptor[] parameters = processor.getParameterDescriptors();
            boolean hasParameterDescriptors = parameters.length > 0;
            if (hasParameterDescriptors) {
                // ignore processor.getDefaultParameter(), if parameter descriptors are given
                defaultParameter = ParametersEditorGenerator.formatAsXMLFromDefaults(parameters);
            }
            processorParametersArea.setValue(defaultParameter);
            processorDescriptionHTML.setHTML(processor.getDescriptionHtml());
            editParametersButton.setEnabled(hasParameterDescriptors);
            if (hasParameterDescriptors) {
                String title = "Edit Parameters for " + processor.getProcessorName() + " v" + processor.getProcessorVersion();
                final ParametersEditorGenerator parametersEditorGenerator = new ParametersEditorGenerator(title, parameters, style);
                editParamsHandlerRegistration = editParametersButton.addClickHandler(new ClickHandler() {
                    @Override
                    public void onClick(ClickEvent event) {
                        String textboxValue = processorParametersArea.getValue().trim();
                        if (!textboxValue.isEmpty()) {
                            parametersEditorGenerator.setFromXML(textboxValue);
                        }
                        parametersEditorGenerator.showDialog("800px", "640px", new ParametersEditorGenerator.OnOkHandler() {
                            @Override
                            public void onOk() {
                                String xml = parametersEditorGenerator.formatAsXMLFromWidgets();
                                processorParametersArea.setValue(xml);
                            }
                        });
                    }
                });
            }
        } else {
            processorBundleName.setHTML("");
            processorParametersArea.setValue("");
            processorDescriptionHTML.setHTML("");
        }
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
        return getValueMap("");
    }

    public Map<String, String> getValueMap(String suffix) {
        Map<String, String> parameters = new HashMap<String, String>();
        DtoProcessorDescriptor processorDescriptor = getSelectedProcessorDescriptor();
        if (processorDescriptor != null) {
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME + suffix, processorDescriptor.getBundleName());
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION + suffix, processorDescriptor.getBundleVersion());
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION + suffix, processorDescriptor.getBundleLocation());
            parameters.put(ProcessorProductionRequest.PROCESSOR_NAME + suffix, processorDescriptor.getExecutableName());
            parameters.put(ProcessorProductionRequest.PROCESSOR_PARAMETERS + suffix, getProcessorParameters());
        }
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        String bundleNameValue = parameters.get(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME);
        String bundleVersionValue = parameters.get(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION);
        String bundleLocationValue = parameters.get(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION);
        String processorNameValue = parameters.get(ProcessorProductionRequest.PROCESSOR_NAME);
        String processorParameterValue = parameters.get(ProcessorProductionRequest.PROCESSOR_PARAMETERS);

        boolean processorSelected = false;
        if (bundleNameValue != null && bundleVersionValue != null && processorNameValue != null) {
            int selectionIndex = findProcessor(processorDescriptors,
                                               bundleNameValue, bundleVersionValue,
                                               bundleLocationValue, processorNameValue);
            if (selectionIndex > -1) {
                processorList.setSelectedIndex(selectionMandatory ? selectionIndex : selectionIndex + 1);
                updateProcessorDetails();
                if (processorParameterValue != null) {
                    processorParametersArea.setValue(processorParameterValue);
                }
                processorSelected = true;
            }
        }
        if (!processorSelected) {
            // no matching processor found, select the first
            processorList.setSelectedIndex(0);
            updateProcessorDetails();
        }
        // TODO handle failure
    }

    private int findProcessor(List<DtoProcessorDescriptor> descriptorList,
                              String bundleName, String bundleVersion, String bundleLocation, String processorName) {
        for (int i = 0; i < descriptorList.size(); i++) {
            DtoProcessorDescriptor processorDescriptor = descriptorList.get(i);
            if (bundleName.equals(processorDescriptor.getBundleName()) &&
                    bundleVersion.equals(processorDescriptor.getBundleVersion()) &&
                    processorName.equals(processorDescriptor.getExecutableName()) &&
                    (bundleLocation == null || bundleLocation.equals(processorDescriptor.getBundleLocation()))) {
                return i;
            }
        }
        return -1;
    }

    private static class L2ProcessorFilter implements Filter<DtoProcessorDescriptor> {

        @Override
        public boolean accept(DtoProcessorDescriptor dtoProcessorDescriptor) {
            return dtoProcessorDescriptor.getProcessorCategory() == DtoProcessorDescriptor.DtoProcessorCategory.LEVEL2;
        }
    }

}