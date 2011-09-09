package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.*;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class ProcessorParametersForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProcessorParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    HTML title;
    @UiField
    TextArea processorParametersArea;
    @UiField
    HTML processorDescriptionHTML;
    @UiField
    FileUpload fileUpload;
    @UiField
    FormPanel uploadForm;


    public ProcessorParametersForm(String title) {
        initWidget(uiBinder.createAndBindUi(this));

        this.title.setHTML(title);

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
    }

    public String getProcessorParameters() {
        return processorParametersArea.getValue();
    }

    public void setProcessorDescriptor(DtoProcessorDescriptor processorDescriptor) {
        if (processorDescriptor != null) {
            processorParametersArea.setValue(processorDescriptor.getDefaultParameter());
            processorDescriptionHTML.setHTML(processorDescriptor.getDescriptionHtml());
        } else {
            processorParametersArea.setValue("");
            processorDescriptionHTML.setHTML("");
        }
    }

    public void validateForm() throws ValidationException {
    }

}