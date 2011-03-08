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

import static com.bc.calvalus.portal.client.CalvalusPortal.*;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class GeneralProcessorPanel implements IsWidget {
    public static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";
    private ListBox processorName;
    private TextArea processorParameters;
    private ListBox bundleVersion;
    private FileUpload fileUpload;
    private final CalvalusPortal portal;
    private DecoratorPanel widget;
    private FormPanel uploadForm;

    public GeneralProcessorPanel(CalvalusPortal portal, String title) {
        this.portal = portal;

        processorName = new ListBox();
        processorName.setName("processorName");
        processorName.setWidth("20em");
        for (GsProcessorDescriptor processor : portal.getProcessors()) {
            String label = processor.getProcessorName() + " (from " + processor.getBundleName() + ")";
            this.processorName.addItem(label, processor.getExecutableName());
        }
        if (portal.getProcessors().length > 0) {
            processorName.setSelectedIndex(0);
        }
        processorName.setVisibleItemCount(3);
        processorName.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateParametersWidget();
                updateProcessorVersionsWidget();
            }
        });

        processorParameters = new TextArea();
        processorParameters.setName("processorParameters");
        processorParameters.setCharacterWidth(48);
        processorParameters.setVisibleLines(16);

        bundleVersion = new ListBox();
        bundleVersion.setName("bundleVersion");
        bundleVersion.setVisibleItemCount(3);

        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        fileUpload.addChangeHandler(new FileUploadChangeHandler());
        fileUpload.setWidth("30em");

        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(2);
        processorPanel.add(createLabeledWidgetV("Processor:", processorName));
        processorPanel.add(createLabeledWidgetV("Bundle version:", bundleVersion));

        uploadForm = new FormPanel();
        uploadForm.setWidget(createLabeledWidgetH("From file:", fileUpload));
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
        processorPanel.setSpacing(2);
        processorParamsPanel.add(processorParameters);
        processorParamsPanel.add(uploadForm);

        // Add advanced options to form in a disclosure panel
        DisclosurePanel advancedDisclosure = new DisclosurePanel("Parameters");
        advancedDisclosure.setAnimationEnabled(true);
        advancedDisclosure.ensureDebugId("cwDisclosurePanel");
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

        if (portal.getProcessors().length > 0) {
            updateParametersWidget();
            updateProcessorVersionsWidget();
        }
    }

    public GsProcessorDescriptor getSelectedProcessor() {
        int selectedIndex = processorName.getSelectedIndex();
        return portal.getProcessors()[selectedIndex];
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
        processorParameters.setValue(getSelectedProcessor().getDefaultParameters());
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