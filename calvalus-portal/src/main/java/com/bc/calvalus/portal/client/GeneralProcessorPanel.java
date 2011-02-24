package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProcessor;
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
    private ListBox processor;
    private ListBox processorVersion;
    private TextArea processorParameters;
    private FileUpload fileUpload;
    private final CalvalusPortal portal;
    private DecoratorPanel widget;
    private FormPanel uploadForm;

    public GeneralProcessorPanel(CalvalusPortal portal, String title) {
        this.portal = portal;

        processor = new ListBox();
        processor.setName("processorListBox");
        for (PortalProcessor processor : portal.getProcessors()) {
            this.processor.addItem(processor.getName(), processor.getOperator());
        }
        if (portal.getProcessors().length > 0) {
            processor.setSelectedIndex(0);
        }
        processor.setVisibleItemCount(3);
        processor.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateProcessorVersionsListBox();
            }
        });

        processorVersion = new ListBox();
        processorVersion.setName("processorVersionListBox");
        if (portal.getProcessors().length > 0) {
            updateProcessorVersionsListBox();
        }
        processorVersion.setVisibleItemCount(3);

        processorParameters = new TextArea();
        processorParameters.setName("parameterKeyValuesArea");
        processorParameters.setCharacterWidth(48);
        processorParameters.setVisibleLines(16);

        fileUpload = new FileUpload();
        fileUpload.setName("uploadFormElement");
        fileUpload.addChangeHandler(new FileUploadChangeHandler());

        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(2);
        processorPanel.add(createLabeledWidgetV("Processor:", processor));
        processorPanel.add(createLabeledWidgetV("Version:", processorVersion));

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
    }

    private String cleanFromHtml(String results) {
        return results.replace("<pre>", "").replace("</pre>", "").replace("&lt;", "<").replace("&gt;", ">");
    }

    public String getProcessorId() {
        int processorIndex = processor.getSelectedIndex();
        return processor.getValue(processorIndex);

    }

    public String getProcessorVersion() {
        int processorVersionIndex = processorVersion.getSelectedIndex();
        return processorVersion.getValue(processorVersionIndex);
    }

    public String getProcessorParameters() {
        return processorParameters.getText().trim();
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    void updateProcessorVersionsListBox() {
        processorVersion.clear();
        int selectedIndex = processor.getSelectedIndex();
        PortalProcessor selectedProcessor = portal.getProcessors()[selectedIndex];
        String[] versions = selectedProcessor.getVersions();
        for (String version : versions) {
            processorVersion.addItem(version);
        }
        if (versions.length > 0) {
            processorVersion.setSelectedIndex(0);
        }
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