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
public class GeneralProcessorForm implements IsWidget {
    public static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";
    private ListBox processorName;
    private TextArea processorParameters;
    private ListBox bundleVersion;
    private FileUpload fileUpload;
    private DecoratorPanel widget;
    private FormPanel uploadForm;
    private GsProcessorDescriptor[] processors;

    public GeneralProcessorForm(GsProcessorDescriptor[] processors, String title) {
        this.processors = processors;

        processorName = new ListBox();
        processorName.setName("processorName");
        processorName.setWidth("25em");
        for (GsProcessorDescriptor processor : processors) {
            String label = processor.getProcessorName() + " (from " + processor.getBundleName() + ")";
            this.processorName.addItem(label, processor.getExecutableName());
        }
        if (this.processors.length > 0) {
            processorName.setSelectedIndex(0);
        }
        processorName.setVisibleItemCount(3);
        processorName.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateProcessorVersionsWidget();
                updateParametersWidget();
            }
        });

        processorParameters = new TextArea();
        processorParameters.setName("processorParameters");
        processorParameters.setWidth("35em");
        processorParameters.setVisibleLines(16);

        bundleVersion = new ListBox();
        bundleVersion.setName("bundleVersion");
        bundleVersion.setVisibleItemCount(3);
        bundleVersion.setWidth("10em");
        bundleVersion.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateParametersWidget();
            }
        });

        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        fileUpload.addChangeHandler(new FileUploadChangeHandler());

        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(2);
        processorPanel.add(UIUtils.createLabeledWidgetV("Processor:", processorName));
        processorPanel.add(UIUtils.createLabeledWidgetV("Version:", bundleVersion));

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

        FlexTable layout = new FlexTable();
        layout.setWidth("100%");
        layout.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.setCellSpacing(4);
        layout.setWidget(0, 0, new HTML("<b>" + title + "</b>"));
        layout.setWidget(1, 0, processorPanel);
        layout.setWidget(2, 0, advancedDisclosure);

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle(title); //todo - check why title doesn't show
        widget.setWidget(layout);

        if (this.processors.length > 0) {
            updateProcessorVersionsWidget();
            updateParametersWidget();
        }
    }

    public GsProcessorDescriptor getSelectedProcessor() {
        int selectedIndex = processorName.getSelectedIndex();
        return processors[selectedIndex];
    }

    public String getBundleVersion() {
        int processorVersionIndex = bundleVersion.getSelectedIndex();
        return bundleVersion.getValue(processorVersionIndex);
    }

    public String getProcessorParameters() {
        return processorParameters.getText().trim();
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    void updateProcessorVersionsWidget() {
        bundleVersion.clear();
        GsProcessorDescriptor selectedProcessor = getSelectedProcessor();
        String[] versions = selectedProcessor.getBundleVersions();
        for (String version : versions) {
            bundleVersion.addItem(version);
        }
        if (versions.length > 0) {
            bundleVersion.setSelectedIndex(0);
        }
    }

    private void updateParametersWidget() {
        int selectedIndex = bundleVersion.getSelectedIndex();
        if (selectedIndex >= 0) {
            String[] defaultParameters = getSelectedProcessor().getDefaultParameters();
            processorParameters.setValue(defaultParameters[selectedIndex]);
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
}