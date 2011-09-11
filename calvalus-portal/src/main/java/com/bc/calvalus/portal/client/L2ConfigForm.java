package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L2ConfigForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, L2ConfigForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    HTML processorListTitle;
    @UiField
    ListBox processorList;

    @UiField
    HTML processorParametersTitle;
    @UiField
    TextArea processorParametersArea;
    @UiField
    HTML processorDescriptionHTML;
    @UiField
    FileUpload fileUpload;
    @UiField
    FormPanel uploadForm;

    private DtoProcessorDescriptor[] processorDescriptors;
    private final boolean selectionMandatory;

    public L2ConfigForm(PortalContext portalContext, boolean selectionMandatory) {
        this.selectionMandatory = selectionMandatory;
        initWidget(uiBinder.createAndBindUi(this));

        processorDescriptors = portalContext.getProcessors();


        FileUploadManager.submitOnChange(uploadForm, fileUpload, "echo=1",
                                         new FormPanel.SubmitHandler() {
                                             public void onSubmit(FormPanel.SubmitEvent event) {
                                                 // we can check for valid input here
                                             }
                                         },
                                         new FormPanel.SubmitCompleteHandler() {
                                             public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                                                 String results = event.getResults();
                                                 processorParametersArea.setText(results != null ? results : "");
                                             }
                                         }
        );


        setProcessorDescriptor(getProcessorDescriptor());

        initProcessorList();
    }

    private void initProcessorList() {
        if (!selectionMandatory) {
            processorList.addItem("<none>");
        }

        for (DtoProcessorDescriptor processor : processorDescriptors) {
            String label = processor.getBundleName() + "-" + processor.getBundleVersion();
            this.processorList.addItem(label);
        }
        if (processorDescriptors.length > 0) {
            processorList.setSelectedIndex(0);
        }
        processorList.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                setProcessorDescriptor(getProcessorDescriptor());
            }
        });
    }

    public HandlerRegistration addChangeHandler(final ChangeHandler changeHandler) {
        return processorList.addChangeHandler(changeHandler);
    }

    public DtoProcessorDescriptor getProcessorDescriptor() {
        int selectedIndex = processorList.getSelectedIndex();
        if (selectedIndex >= (selectionMandatory ? 0 : 1)) {
            return processorDescriptors[selectedIndex];
        } else {
            return null;
        }
    }

    public String getProcessorParameters() {
        return processorParametersArea.getValue();
    }

    public void validateForm() throws ValidationException {
        DtoProcessorDescriptor processorDescriptor = getProcessorDescriptor();
        boolean processorDescriptorValid = !selectionMandatory || processorDescriptor != null;
        if (!processorDescriptorValid) {
            throw new ValidationException(processorList, "No processor selected.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        DtoProcessorDescriptor processorDescriptor = getProcessorDescriptor();
        if (processorDescriptor != null) {
            parameters.put("processorBundleName", processorDescriptor.getBundleName());
            parameters.put("processorBundleVersion", processorDescriptor.getBundleVersion());
            parameters.put("processorName", processorDescriptor.getExecutableName());
            parameters.put("processorParameters", getProcessorParameters());
        }
        return parameters;
    }


    private void setProcessorDescriptor(DtoProcessorDescriptor processorDescriptor) {
        if (processorDescriptor != null) {
            processorParametersArea.setValue(processorDescriptor.getDefaultParameter());
            processorDescriptionHTML.setHTML(processorDescriptor.getDescriptionHtml());
        } else {
            processorParametersArea.setValue("");
            processorDescriptionHTML.setHTML("");
        }
    }
}