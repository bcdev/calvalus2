package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.DisclosurePanel;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class ProcessorParametersForm implements IsWidget {
    public static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";
    private ListBox processorName;
    private TextArea processorParameters;
    private FileUpload fileUpload;
    private DecoratorPanel widget;
    private FormPanel uploadForm;
    private GsProcessorDescriptor[] processors;
    private HTML processorDescription;

    public ProcessorParametersForm(GsProcessorDescriptor[] processors, String title) {
        this.processors = processors;

        processorName = new ListBox();
        processorName.setName("processorName");
        processorName.setWidth("30em");
        for (GsProcessorDescriptor processor : processors) {
            String label = processor.getProcessorName() + " (from " + processor.getBundleName() + "-" + processor.getBundleVersion() + ")";
            this.processorName.addItem(label, processor.getExecutableName());
        }
        if (this.processors.length > 0) {
            processorName.setSelectedIndex(0);
        }
        processorName.setVisibleItemCount(3);
        processorName.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateParametersWidget();
            }
        });

        processorParameters = new TextArea();
        processorParameters.setName("processorParameters");
        processorParameters.setWidth("35em");
        processorParameters.setVisibleLines(16);

        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        fileUpload.addChangeHandler(new FileUploadChangeHandler());

        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(2);
        processorPanel.add(UIUtils.createLabeledWidgetV("Processor:", processorName));

        uploadForm = new FormPanel();
        uploadForm.setWidget(UIUtils.createLabeledWidgetH("From file:", fileUpload));
        uploadForm.setWidth("35em");
        uploadForm.addSubmitHandler(new FormPanel.SubmitHandler() {
            public void onSubmit(FormPanel.SubmitEvent event) {
                // todo - check inputs
            }
        });
        uploadForm.addSubmitCompleteHandler(new FormPanel.SubmitCompleteHandler() {
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                String results = event.getResults();
                processorParameters.setText(results != null ? results : "");
            }
        });


        VerticalPanel processorParamsPanel = new VerticalPanel();
        processorParamsPanel.ensureDebugId("processorParamsPanel");
        processorPanel.setSpacing(2);
        processorParamsPanel.add(processorParameters);
        processorParamsPanel.add(uploadForm);

        // Add advanced options to form in a disclosure panel
        DisclosurePanel advancedDisclosure = new DisclosurePanel("Parameters");
        advancedDisclosure.ensureDebugId("advancedDisclosure");
        advancedDisclosure.setAnimationEnabled(true);
        advancedDisclosure.setContent(processorParamsPanel);

        processorDescription = new HTML();

        FlexTable layout = new FlexTable();
        layout.setWidth("100%");
        layout.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.setCellSpacing(4);
        layout.setWidget(0, 0, new HTML("<b>" + title + "</b>"));
        layout.setWidget(1, 0, processorPanel);
        layout.setWidget(2, 0, processorDescription);
        layout.setWidget(3, 0, advancedDisclosure);

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle(title); //todo - check why title doesn't show
        widget.setWidget(layout);

        if (this.processors.length > 0) {
            updateParametersWidget();
        }
    }

    public void addProcessorChangedHandler(final ProcessorChangedHandler changedHandler) {
        processorName.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent changeEvent) {
                changedHandler.onProcessorChanged(getSelectedProcessor());
            }
        });
    }

    public GsProcessorDescriptor getSelectedProcessor() {
        int selectedIndex = processorName.getSelectedIndex();
        return processors[selectedIndex];
    }

    public String getProcessorParameters() {
        return processorParameters.getText().trim();
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    private void updateParametersWidget() {
        int selectedIndex = processorName.getSelectedIndex();
        if (selectedIndex >= 0) {
            GsProcessorDescriptor selectedProcessor = getSelectedProcessor();
            processorParameters.setValue(selectedProcessor.getDefaultParameter());
            processorDescription.setHTML(selectedProcessor.getDescriptionHtml());
        }
    }

    public void validateForm() throws ValidationException {
    }

    private class FileUploadChangeHandler implements ChangeHandler {

        public void onChange(ChangeEvent event) {
            String filename = fileUpload.getFilename();
            if (filename != null && !filename.isEmpty()) {
                // Because we're going to add a FileUpload widget, we'll need to set the
                // form to use the POST method, and multi-part MIME encoding.
                uploadForm.setAction(UPLOAD_ACTION_URL + "?echo=1");
                uploadForm.setEncoding(FormPanel.ENCODING_MULTIPART);
                uploadForm.setMethod(FormPanel.METHOD_POST);
                uploadForm.submit();
            }
        }
    }

    public static interface ProcessorChangedHandler {
        void onProcessorChanged(GsProcessorDescriptor gsProcessorDescriptor);
    }

}