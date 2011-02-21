package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import static com.bc.calvalus.portal.client.CalvalusPortal.*;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OrderL2ProductionView extends PortalView {
    public static final int ID = 1;
    public static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";
    FormPanel form;
    ListBox processorListBox;
    ListBox processorVersionListBox;
    ListBox inputPsListBox;
    TextArea parametersTextArea;
    FileUpload parametersFileUpload;
    TextBox outputPsTextBox;

    public OrderL2ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputPsListBox = new ListBox();
        inputPsListBox.setName("productSetListBox");
        for (PortalProductSet productSet : calvalusPortal.getProductSets()) {
            inputPsListBox.addItem(productSet.getName(), productSet.getId());
        }
        inputPsListBox.setVisibleItemCount(6);

        outputPsTextBox = new TextBox();
        outputPsTextBox.setName("productSetNameBox");

        processorListBox = new ListBox();
        processorListBox.setName("processorListBox");
        for (PortalProcessor processor : calvalusPortal.getProcessors()) {
            processorListBox.addItem(processor.getName(), processor.getId());
        }
        if (calvalusPortal.getProcessors().length > 0) {
            processorListBox.setSelectedIndex(0);
        }
        processorListBox.setVisibleItemCount(3);
        processorListBox.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateProcessorVersionsListBox();
            }
        });

        processorVersionListBox = new ListBox();
        processorVersionListBox.setName("processorVersionListBox");
        if (calvalusPortal.getProcessors().length > 0) {
            updateProcessorVersionsListBox();
        }
        processorVersionListBox.setVisibleItemCount(3);

        parametersTextArea = new TextArea();
        parametersTextArea.setName("parameterKeyValuesArea");
        parametersTextArea.setCharacterWidth(48);
        parametersTextArea.setVisibleLines(16);

        parametersFileUpload = new FileUpload();
        parametersFileUpload.setName("uploadFormElement");
        parametersFileUpload.addChangeHandler(new FileUploadChangeHandler());

        Button submitButton = new Button("Submit", new SubmitHandler());

        VerticalPanel productSetPanel = new VerticalPanel();
        productSetPanel.setSpacing(4);
        productSetPanel.add(createLabeledWidget("Input Level 1 product set:", inputPsListBox));
        productSetPanel.add(createLabeledWidget("Output Level 2 product set:", outputPsTextBox));

        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(4);
        processorPanel.add(createLabeledWidget("Processor:", processorListBox));
        processorPanel.add(createLabeledWidget("Version:", processorVersionListBox));

        VerticalPanel processorAndParamsPanel = new VerticalPanel();
        processorPanel.setSpacing(4);
        processorAndParamsPanel.add(processorPanel);
        processorAndParamsPanel.add(createLabeledWidget("Processing parameters:", parametersTextArea));
        processorAndParamsPanel.add(createLabeledWidget("Parameter file:", parametersFileUpload));

        FlexTable flexTable = new FlexTable();
        flexTable.getFlexCellFormatter().setVerticalAlignment(0, 0, HasVerticalAlignment.ALIGN_TOP);
        flexTable.getFlexCellFormatter().setVerticalAlignment(0, 1, HasVerticalAlignment.ALIGN_TOP);
        flexTable.getFlexCellFormatter().setHorizontalAlignment(1, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        flexTable.getFlexCellFormatter().setColSpan(1, 0, 2);
        flexTable.ensureDebugId("cwFlexTable");
        flexTable.addStyleName("cw-FlexTable");
        flexTable.setWidth("32em");
        flexTable.setCellSpacing(2);
        flexTable.setCellPadding(2);
        flexTable.setWidget(0, 0, productSetPanel);
        flexTable.setWidget(0, 1, processorAndParamsPanel);
        flexTable.setWidget(1, 0, submitButton);

        form = new FormPanel();
        form.setWidget(flexTable);

        form.addSubmitHandler(new FormPanel.SubmitHandler() {
            public void onSubmit(FormPanel.SubmitEvent event) {
                // todo - check inputs
            }
        });
        form.addSubmitCompleteHandler(new FormPanel.SubmitCompleteHandler() {
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                parametersTextArea.setText(event.getResults());
            }
        });

    }

    @Override
    public Widget asWidget() {
        return form;
    }

    @Override
    public int getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Order L2 Production";
    }

    void updateProcessorVersionsListBox() {
        processorVersionListBox.clear();
        int selectedIndex = processorListBox.getSelectedIndex();
        PortalProcessor selectedProcessor = getPortal().getProcessors()[selectedIndex];
        String[] versions = selectedProcessor.getVersions();
        for (String version : versions) {
            processorVersionListBox.addItem(version);
        }
        if (versions.length > 0) {
            processorVersionListBox.setSelectedIndex(0);
        }
    }

    private class SubmitHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            int productSetIndex = inputPsListBox.getSelectedIndex();
            int processorIndex = processorListBox.getSelectedIndex();
            int processorVersionIndex = processorVersionListBox.getSelectedIndex();
            PortalProductionRequest request = new PortalProductionRequest(inputPsListBox.getValue(productSetIndex),
                                                                          outputPsTextBox.getText().trim(),
                                                                          processorListBox.getValue(processorIndex),
                                                                          processorVersionListBox.getValue(processorVersionIndex),
                                                                          parametersTextArea.getText().trim());
            getPortal().getBackendService().orderProduction(request, new AsyncCallback<PortalProductionResponse>() {
                public void onSuccess(final PortalProductionResponse response) {
                    ManageProductionsView view = (ManageProductionsView) getPortal().getView(ManageProductionsView.ID);
                    view.addProduction(response.getProduction());
                    view.show();
                }

                public void onFailure(Throwable caught) {
                    Window.alert("Error!\n" + caught.getMessage());
                }
            });
        }

    }

    private class FileUploadChangeHandler implements ChangeHandler {

        public void onChange(ChangeEvent event) {
            String filename = parametersFileUpload.getFilename();
            if (filename != null && !filename.isEmpty()) {
                // Because we're going to add a FileUpload widget, we'll need to set the
                // form to use the POST method, and multi-part MIME encoding.
                form.setAction(UPLOAD_ACTION_URL);
                form.setEncoding(FormPanel.ENCODING_MULTIPART);
                form.setMethod(FormPanel.METHOD_POST);
                form.submit();
            }
        }
    }
}