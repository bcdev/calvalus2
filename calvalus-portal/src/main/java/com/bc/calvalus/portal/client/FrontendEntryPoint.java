package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.smartgwt.client.widgets.Label;
import org.gwtopenmaps.openlayers.client.LonLat;
import org.gwtopenmaps.openlayers.client.MapOptions;
import org.gwtopenmaps.openlayers.client.MapWidget;
import org.gwtopenmaps.openlayers.client.control.LayerSwitcher;
import org.gwtopenmaps.openlayers.client.control.MousePosition;
import org.gwtopenmaps.openlayers.client.control.NavToolbar;
import org.gwtopenmaps.openlayers.client.control.PanZoomBar;
import org.gwtopenmaps.openlayers.client.layer.Layer;
import org.gwtopenmaps.openlayers.client.layer.TransitionEffect;
import org.gwtopenmaps.openlayers.client.layer.WMS;
import org.gwtopenmaps.openlayers.client.layer.WMSOptions;
import org.gwtopenmaps.openlayers.client.layer.WMSParams;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class FrontendEntryPoint implements EntryPoint {

    static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";

    private final BackendServiceAsync backendService = GWT.create(BackendService.class);

    private FormPanel form;
    private ListBox processorListBox;
    private ListBox processorVersionListBox;
    private ListBox inputPsListBox;
    private TextArea parametersTextArea;
    private FileUpload parametersFileUpload;
    private TextBox outputPsTextBox;

    private PortalProductSet[] productSets;
    private PortalProcessor[] processors;
    private boolean initialised;

    /**
     * This is the entry point method.
     */
    public void onModuleLoad() {

        final DialogBox splashScreen = createSplashScreen();

        backendService.getProductSets(null, new AsyncCallback<PortalProductSet[]>() {
            @Override
            public void onSuccess(PortalProductSet[] productSets) {
                FrontendEntryPoint.this.productSets = productSets;
                maybeInitFrontend(splashScreen);
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Error!\n" + caught.getMessage());
                FrontendEntryPoint.this.productSets = new PortalProductSet[0];
            }
        });

        backendService.getProcessors(null, new AsyncCallback<PortalProcessor[]>() {
            @Override
            public void onSuccess(PortalProcessor[] processors) {
                FrontendEntryPoint.this.processors = processors;
                maybeInitFrontend(splashScreen);
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Error!\n" + caught.getMessage());
                FrontendEntryPoint.this.processors = new PortalProcessor[0];
            }
        });
    }

    private DialogBox createSplashScreen() {
        final DialogBox splashScreen = new DialogBox();
        splashScreen.setText("Calvalus");
        splashScreen.add(new Label("Please wait, while Calvalus is loading..."));
        splashScreen.setModal(true);
        splashScreen.setAnimationEnabled(true);
        splashScreen.setGlassEnabled(true);
        splashScreen.setSize("320", "200");
        splashScreen.showRelativeTo(RootPanel.get());
        return splashScreen;
    }


    private void maybeInitFrontend(DialogBox splashScreen) {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            splashScreen.hide();
            DOM.removeChild(RootPanel.getBodyElement(), DOM.getElementById("loading"));
            initFrontend();
        }
    }

    private void initFrontend() {
        inputPsListBox = new ListBox();
        inputPsListBox.setName("productSetListBox");
        for (PortalProductSet productSet : productSets) {
            inputPsListBox.addItem(productSet.getName(), productSet.getId());
        }
        inputPsListBox.setVisibleItemCount(6);

        outputPsTextBox = new TextBox();
        outputPsTextBox.setName("productSetNameBox");

        processorListBox = new ListBox();
        processorListBox.setName("processorListBox");
        for (PortalProcessor processor : processors) {
            processorListBox.addItem(processor.getName(), processor.getId());
        }
        if (processors.length > 0) {
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
        if (processors.length > 0) {
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

        VerticalPanel productSetPanelA = new VerticalPanel();
        productSetPanelA.setSpacing(4);
        productSetPanelA.add(new Label("Input Level 1 product set:"));
        productSetPanelA.add(inputPsListBox);
        VerticalPanel productSetPanelB = new VerticalPanel();
        productSetPanelB.setSpacing(4);
        productSetPanelB.add(new Label("Output Level 2 product set:"));
        productSetPanelB.add(outputPsTextBox);
        HorizontalPanel productSetPanel = new HorizontalPanel();
        productSetPanel.setSpacing(4);
        productSetPanel.add(productSetPanelA);
        productSetPanel.add(productSetPanelB);

        VerticalPanel processorPanelA = new VerticalPanel();
        processorPanelA.setSpacing(4);
        processorPanelA.add(new Label("Processor:"));
        processorPanelA.add(processorListBox);
        VerticalPanel processorPanelB = new VerticalPanel();
        processorPanelB.setSpacing(4);
        processorPanelB.add(new Label("Version:"));
        processorPanelB.add(processorVersionListBox);
        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(4);
        processorPanel.add(processorPanelA);
        processorPanel.add(processorPanelB);

        // Create a panel to hold all of the form widgets.
        final VerticalPanel formPanel = new VerticalPanel();
        formPanel.setStyleName("formPanel");

        formPanel.add(productSetPanel);
        formPanel.add(processorPanel);
        formPanel.add(new Label("<b>Processing parameters:"));
        formPanel.add(parametersTextArea);
        formPanel.add(new Label("<b>Parameter file:"));
        formPanel.add(parametersFileUpload);
        formPanel.add(submitButton);
        formPanel.add(new HTML("<p/><p/><p/>"));
        formPanel.add(new Label("Map:"));
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
    }

    private void updateProcessorVersionsListBox() {
        processorVersionListBox.clear();
        int selectedIndex = processorListBox.getSelectedIndex();
        PortalProcessor selectedProcessor = processors[selectedIndex];
        String[] versions = selectedProcessor.getVersions();
        for (String version : versions) {
            processorVersionListBox.addItem(version);
        }
        if (versions.length > 0) {
            processorVersionListBox.setSelectedIndex(0);
        }
    }

    private boolean isAllInputDataAvailable() {
        return productSets != null && processors != null;
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
            backendService.orderProduction(request, new AsyncCallback<PortalProductionResponse>() {
                public void onSuccess(PortalProductionResponse response) {
                    Window.alert("Success!\n" + response.getMessage());
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



