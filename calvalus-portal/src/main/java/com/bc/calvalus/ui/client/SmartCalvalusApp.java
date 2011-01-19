package com.bc.calvalus.ui.client;

import com.bc.calvalus.ui.shared.ProcessingRequest;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.smartgwt.client.types.Alignment;
import com.smartgwt.client.util.SC;
import com.smartgwt.client.widgets.IButton;
import com.smartgwt.client.widgets.Window;
import com.smartgwt.client.widgets.events.ClickEvent;
import com.smartgwt.client.widgets.events.ClickHandler;
import com.smartgwt.client.widgets.events.CloseClickHandler;
import com.smartgwt.client.widgets.events.CloseClientEvent;
import com.smartgwt.client.widgets.form.DynamicForm;
import com.smartgwt.client.widgets.form.fields.ComboBoxItem;
import com.smartgwt.client.widgets.form.fields.HeaderItem;
import com.smartgwt.client.widgets.form.fields.TextAreaItem;
import com.smartgwt.client.widgets.form.fields.TextItem;
import com.smartgwt.client.widgets.layout.VLayout;

import java.util.LinkedHashMap;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class SmartCalvalusApp implements EntryPoint {

    static final String PROCESS_ACTION_URL = GWT.getModuleBaseURL() + "process";
    static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";

    private final ProcessingServiceAsync processingService = GWT.create(ProcessingService.class);

    private ComboBoxItem processorListBox;
    private ComboBoxItem inputPsListBox;
    private TextAreaItem parametersTextArea;
    private TextItem outputPsTextBox;

    /**
     * This is the entry point method.
     */
    public void onModuleLoad() {

        GWT.setUncaughtExceptionHandler(new GWT.UncaughtExceptionHandler() {
            public void onUncaughtException(Throwable e) {
                e.printStackTrace(System.err);
                SC.warn("Internal error: " + e.getMessage());
            }
        });

        VLayout layout = new VLayout(10);

        DynamicForm form = new DynamicForm();

        LinkedHashMap<String, String> psMap = new LinkedHashMap<String, String>();
        psMap.put("MERIS RR 2004-2009", "MERIS RR 2004-2009");
        psMap.put("MERIS RR 2004", "MERIS RR 2004");
        psMap.put("MERIS RR 2005", "MERIS RR 2005");
        psMap.put("MERIS RR 2006", "MERIS RR 2006");
        psMap.put("MERIS RR 2007", "MERIS RR 2007");
        psMap.put("MERIS RR 2008", "MERIS RR 2008");
        psMap.put("MERIS RR 2009", "MERIS RR 2009");

        LinkedHashMap<String, String> processorMap = new LinkedHashMap<String, String>();
        processorMap.put("c2r", "Case 2 Regional");
        processorMap.put("qaa", "QAA");
        processorMap.put("l2gen", "l2gen");

        HeaderItem headerItem = new HeaderItem();
        headerItem.setDefaultValue("Calvalus Level-2 Processing");

        inputPsListBox = new ComboBoxItem();
        inputPsListBox.setName("inputPsListBox");
        inputPsListBox.setValue("MERIS RR 2008");
        inputPsListBox.setRequired(true);
        inputPsListBox.setValueMap(psMap);
        inputPsListBox.setTitle("Input product set");
        inputPsListBox.setWrapTitle(false);

        outputPsTextBox = new TextItem();
        outputPsTextBox.setName("outputPsTextBox");
        outputPsTextBox.setValue("test-01");
        outputPsTextBox.setRequired(true);
        outputPsTextBox.setTitle("Output product set");
        outputPsTextBox.setWrapTitle(false);

        processorListBox = new ComboBoxItem();
        processorListBox.setName("processorListBox");
        processorListBox.setValue("c2r");
        processorListBox.setRequired(true);
        processorListBox.setValueMap(processorMap);
        processorListBox.setTitle("Level 2 processor");
        processorListBox.setWrapTitle(false);

        parametersTextArea = new TextAreaItem();
        parametersTextArea.setName("parametersTextArea");
        parametersTextArea.setWidth(300);
        parametersTextArea.setHeight(200);
        parametersTextArea.setTitle("Processing parameters");
        parametersTextArea.setWrapTitle(false);

        IButton fileButton = new IButton("...");
        fileButton.setShowRollOver(true);
        fileButton.setShowDown(true);
        fileButton.setAlign(Alignment.RIGHT);
        fileButton.addClickHandler(new FileAction());

        IButton submitButton = new IButton("Process");
        submitButton.addClickHandler(new InvokeProcessorAction());

        form.setFields(headerItem, inputPsListBox, outputPsTextBox, processorListBox, parametersTextArea);
        layout.addMember(form);
        layout.addMember(fileButton);
        layout.addMember(submitButton);


        /*
        //Defining a WMSLayer and adding it to a Map
        WMSParams wmsParams = new WMSParams();
        wmsParams.setFormat("image/png");
        wmsParams.setLayers("basic");
        wmsParams.setStyles("");

        WMSOptions wmsLayerParams = new WMSOptions();
        wmsLayerParams.setUntiled();
        wmsLayerParams.setTransitionEffect(TransitionEffect.RESIZE);

        WMS wmsLayer = new WMS(
                "Basic WMS",
                "http://labs.metacarta.com/wms/vmap0",
                wmsParams,
                wmsLayerParams);

        MapOptions defaultMapOptions = new MapOptions();
        //In OL, the map gets PanZoom, Navigation, ArgParser, and Attribution Controls
        //by default. Do removeDefaultControls to remove these.
        //defaultMapOptions.removeDefaultControls();
        defaultMapOptions.setNumZoomLevels(16);
        defaultMapOptions.setProjection("EPSG:4326");

        MapWidget mapWidget = new MapWidget("350px", "350px", defaultMapOptions);
        mapWidget.getMap().addLayers(new Layer[]{wmsLayer});

        //Adding controls to the Map
        mapWidget.getMap().addControl(new PanZoomBar());
        //use NavToolbar instead of deprecated MouseToolbar
        mapWidget.getMap().addControl(new NavToolbar());
        mapWidget.getMap().addControl(new MousePosition());
        mapWidget.getMap().addControl(new LayerSwitcher());
        //Center and Zoom
        double lon = 4.0;
        double lat = 5.0;
        int zoom = 5;
        mapWidget.getMap().setCenter(new LonLat(lon, lat), zoom);

        // Create a panel to hold all of the form widgets.
        final VerticalPanel formPanel = new VerticalPanel();
        formPanel.setStyleName("formPanel");
        formPanel.add(new HTML("<b>Input Level 1 product set:</b>"));
        formPanel.add(inputPsListBox);
        formPanel.add(new HTML("<b>Output Level 2 product set:</b>"));
        formPanel.add(outputPsTextBox);
        formPanel.add(new HTML("<b>Level-2 Processor:</b>"));
        formPanel.add(processorListBox);
        formPanel.add(new HTML("<b>Processing parameters:</b>"));
        formPanel.add(parametersTextArea);
        formPanel.add(new HTML("<b>Parameter file:</b>"));
        formPanel.add(parametersFileUpload);
        formPanel.add(submitButton);
        formPanel.add(new HTML("<br/><br/><b>Map:</b>"));
        formPanel.add(mapWidget);


        form = new FormPanel();
        form.setWidget(formPanel);
        form.addSubmitHandler(new FormPanel.SubmitHandler() {
            public void onSubmit(FormPanel.SubmitEvent event) {
                // todo - check inputs
                //Window.alert(GWT.getModuleBaseURL());
            }
        });
        form.addSubmitCompleteHandler(new FormPanel.SubmitCompleteHandler() {
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                parametersTextArea.setText(event.getResults());
            }
        });

        RootPanel.get().add(form);
        */

        layout.draw();
    }


    private class InvokeProcessorAction implements ClickHandler {
        public void onClick(ClickEvent clickEvent) {
            String inputProductSet = (String) inputPsListBox.getValue();
            String outputProductSet = (String) outputPsTextBox.getValue();
            String processorName = (String) processorListBox.getValue();
            String processingParameters = (String) parametersTextArea.getValue();
            ProcessingRequest request = new ProcessingRequest(inputProductSet,
                                                              outputProductSet,
                                                              processorName,
                                                              processingParameters);
            processingService.process(request, new AsyncCallback<String>() {
                public void onSuccess(String result) {
                    SC.say("Success!\n" + result);
                }

                public void onFailure(Throwable caught) {
                    SC.say("Error!\n" + caught.getMessage());
                }
            });
        }
    }

    private class FileAction implements ClickHandler {
        public void onClick(ClickEvent event) {
            final Window parameterFileDialog = new Window();
            final FormPanel uploadForm = new FormPanel();

            final FileUpload parametersFileUpload = new FileUpload();
            parametersFileUpload.setName("parametersFileUpload");
            parametersFileUpload.setTitle("Parameter file");
            parametersFileUpload.addChangeHandler(new com.google.gwt.event.dom.client.ChangeHandler() {
                public void onChange(com.google.gwt.event.dom.client.ChangeEvent changeEvent) {
                    String filename = parametersFileUpload.getFilename();
                    if (filename != null && !filename.isEmpty()) {
                        uploadForm.submit();
                    }
                }
            });

            uploadForm.setWidget(parametersFileUpload);
            uploadForm.addSubmitCompleteHandler(new FormPanel.SubmitCompleteHandler() {
                public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                    parametersTextArea.setValue(event.getResults());
                    parameterFileDialog.hide();
                }
            });
            uploadForm.setAction(UPLOAD_ACTION_URL);
            uploadForm.setEncoding(FormPanel.ENCODING_MULTIPART);
            uploadForm.setMethod(FormPanel.METHOD_POST);
            uploadForm.setAction(UPLOAD_ACTION_URL);

            parameterFileDialog.setWidth(360);
            parameterFileDialog.setHeight(115);
            parameterFileDialog.setTitle("Select Parameter File");
            parameterFileDialog.setShowMinimizeButton(false);
            parameterFileDialog.setIsModal(true);
            parameterFileDialog.setShowModalMask(true);
            parameterFileDialog.centerInPage();
            parameterFileDialog.addCloseClickHandler(new CloseClickHandler() {
                public void onCloseClick(CloseClientEvent event) {
                    parameterFileDialog.hide();
                }
            });
            parameterFileDialog.addItem(uploadForm);
            parameterFileDialog.show();
        }
    }
}



