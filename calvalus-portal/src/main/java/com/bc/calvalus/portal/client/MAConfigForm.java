package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.maps.client.MapOptions;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.base.LatLng;
import com.google.gwt.maps.client.base.LatLngBounds;
import com.google.gwt.maps.client.overlays.Marker;
import com.google.gwt.maps.client.overlays.MarkerImage;
import com.google.gwt.maps.client.overlays.MarkerOptions;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Form for MA (matup analysis) parameters.
 *
 * @author Norman
 */
public class MAConfigForm extends Composite {

    private static final String POINT_DATA_DIR = "point-data";

    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, MAConfigForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox recordSources;
    @UiField
    Button addRecordSourceButton;
    @UiField
    Button checkRecordSourceButton;
    @UiField
    Button viewRecordSourceButton;
    @UiField
    Button removeRecordSourceButton;

    @UiField
    IntegerBox macroPixelSize;
    @UiField
    DoubleBox maxTimeDifference;
    @UiField
    DoubleBox filteredMeanCoeff;
    @UiField
    TextBox outputGroupName;

    @UiField
    TextBox goodPixelExpression;
    @UiField
    TextBox goodRecordExpression;

    private FileUpload fileUpload;
    private FormPanel uploadForm;

    public MAConfigForm(final PortalContext portalContext) {
        this.portalContext = portalContext;

        initWidget(uiBinder.createAndBindUi(this));

        macroPixelSize.setValue(5);
        maxTimeDifference.setValue(3.0);
        filteredMeanCoeff.setValue(1.5);
        outputGroupName.setValue("SITE");

        AddRecordSourceAction addRecordSourceAction = new AddRecordSourceAction();
        addRecordSourceButton.addClickHandler(addRecordSourceAction);
        removeRecordSourceButton.addClickHandler(new RemoveRecordSourceAction());
        checkRecordSourceButton.addClickHandler(new CheckRecordSourceAction());
        viewRecordSourceButton.addClickHandler(new ViewRecordSourceAction());

        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        uploadForm = new FormPanel();
        uploadForm.setWidget(fileUpload);

        FileUploadManager.configureForm(uploadForm,
                                        "dir=" + POINT_DATA_DIR,
                                        addRecordSourceAction,
                                        addRecordSourceAction);

        updateRecordSources();
    }

    private void updateRecordSources() {
        portalContext.getBackendService().listUserFiles(POINT_DATA_DIR, new AsyncCallback<String[]>() {
            @Override
            public void onSuccess(String[] filePaths) {
                setRecordSources(filePaths);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to get list of point data files from server.");
            }
        });
    }

    private void removeRecordSource(final String recordSource) {
        portalContext.getBackendService().removeUserFile(POINT_DATA_DIR + "/" + recordSource, new AsyncCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean removed) {
                updateRecordSources();
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to remove file '" + recordSource + "' from server.");
            }
        });
    }

    private void checkRecordSource(final String recordSource) {
        portalContext.getBackendService().checkUserRecordSource(POINT_DATA_DIR + "/" + recordSource, new AsyncCallback<String>() {
            @Override
            public void onSuccess(String message) {
                Dialog.info("Passed", "parsing " + recordSource + " succeeded: " + message);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Failed", "Failed to parse " + recordSource + ": " + caught.getMessage());
            }
        });
    }

    private void viewRecordSource(final String recordSource) {
        portalContext.getBackendService().listUserRecordSource(POINT_DATA_DIR + "/" + recordSource, new AsyncCallback<float[]>() {
            @Override
            public void onSuccess(float[] points) {
                MapOptions mapOptions = MapOptions.newInstance();
                mapOptions.setCenter(LatLng.newInstance(0.0, 0.0));
                mapOptions.setDisableDoubleClickZoom(false);
                mapOptions.setScrollWheel(true);
                mapOptions.setMapTypeControl(true);
                mapOptions.setZoomControl(true);
                mapOptions.setPanControl(true);
                mapOptions.setStreetViewControl(false);
                final MapWidget mapWidget = new MapWidget(mapOptions);
                mapWidget.setSize("800px", "520px");
                LatLngBounds bounds = null;
                MarkerImage markerImage = MarkerImage.newInstance("https://maps.gstatic.com/intl/en_ALL/mapfiles/markers2/measle.png");
                for (int i = 0; i < points.length; ) {
                    final float lat = points[i++];
                    final float lon = points[i++];
                    LatLng latLngPoint = LatLng.newInstance(lat, lon);
                    if (bounds == null) {
                        bounds = LatLngBounds.newInstance(latLngPoint, latLngPoint);
                    } else {
                        bounds.extend(latLngPoint);
                    }
                    MarkerOptions markerOptions = MarkerOptions.newInstance();
                    markerOptions.setPosition(latLngPoint);
                    Marker marker = Marker.newInstance(markerOptions);
                    marker.setIcon(markerImage);
                    marker.setMap(mapWidget);
                }
                if (bounds != null) {
                    final LatLngBounds finalBounds = bounds;
                    String title = "Viewing " + recordSource + " with " + (points.length / 2) + " measurements";
                    Dialog dialog = new Dialog(title, mapWidget, Dialog.ButtonType.CLOSE) {
                        @Override
                        protected void onShow() {
                            mapWidget.triggerResize();
                            mapWidget.fitBounds(finalBounds);
                            mapWidget.panTo(finalBounds.getCenter());
                        }
                    };
                    dialog.show();
                }
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Failed", "Failed to view " + recordSource + ": " + caught.getMessage());
            }
        });
    }


    private void setRecordSources(String[] filePaths) {
        recordSources.clear();
        for (String filePath : filePaths) {
            int baseDirPos = filePath.lastIndexOf(POINT_DATA_DIR + "/");
            if (baseDirPos >= 0) {
                recordSources.addItem(filePath.substring(baseDirPos + POINT_DATA_DIR.length() + 1), filePath);
            } else {
                recordSources.addItem(filePath);
            }
        }
        if (recordSources.getItemCount() > 0) {
            recordSources.setSelectedIndex(0);
        }
    }

    String getSelectedRecordSourceFilename() {
        int selectedIndex = recordSources.getSelectedIndex();
        if (selectedIndex >= 0) {
            return recordSources.getItemText(selectedIndex);
        }
        return null;
    }

    public void setProcessorDescriptor(DtoProcessorDescriptor selectedProcessor) {
    }

    public void validateForm() throws ValidationException {
        boolean macroPixelSizeValid = macroPixelSize.getValue() >= 1
                && macroPixelSize.getValue() <= 31
                && macroPixelSize.getValue() % 2 == 1;
        if (!macroPixelSizeValid) {
            throw new ValidationException(macroPixelSize, "Macro pixel size must be an odd integer between 1 and 31");
        }
        boolean filteredMeanCoeffValid = filteredMeanCoeff.getValue() >= 0;
        if (!filteredMeanCoeffValid) {
            throw new ValidationException(filteredMeanCoeff, "Filtered mean coefficient must be >= 0 (0 disables this criterion)");
        }

        boolean maxTimeDifferenceValid = maxTimeDifference.getValue() >= 0;
        if (!maxTimeDifferenceValid) {
            throw new ValidationException(maxTimeDifference, "Max. time difference must be >= 0 hours (0 disables this criterion)");
        }

        boolean recordSourceValid = recordSources.getSelectedIndex() >= 0;
        if (!recordSourceValid) {
            throw new ValidationException(maxTimeDifference, "In-situ record source must be given.");
        }

    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("copyInput", "true");
        parameters.put("goodPixelExpression", goodPixelExpression.getText());
        parameters.put("goodRecordExpression", goodRecordExpression.getText());
        parameters.put("macroPixelSize", macroPixelSize.getText());
        parameters.put("maxTimeDifference", maxTimeDifference.getText());
        parameters.put("filteredMeanCoeff", filteredMeanCoeff.getText());
        parameters.put("outputGroupName", outputGroupName.getText());
        int selectedIndex = recordSources.getSelectedIndex();
        parameters.put("recordSourceUrl", selectedIndex >= 0 ? recordSources.getValue(selectedIndex) : "");
        return parameters;
    }


    private class AddRecordSourceAction implements ClickHandler, FormPanel.SubmitHandler, FormPanel.SubmitCompleteHandler {

        private Dialog fileUploadDialog;
        private Dialog monitorDialog;
        private FormPanel.SubmitEvent submitEvent;

        @Override
        public void onClick(ClickEvent event) {
            VerticalPanel verticalPanel = UIUtils.createVerticalPanel(2,
                                                                      new HTML("Select in-situ or point data file:"),
                                                                      uploadForm,
                                                                      new HTML("The supported file types are TAB-separated CSV (<b>*.txt</b>, <b>*.csv</b>)<br/>" +
                                                                                       "and BEAM placemark files (<b>*.placemark</b>)."),
                                                                      new HTML("<h4>Standard format:</h4>" +
                                                                                       "The first line of the TAB-separated CSV file must contain header names. <br/>" +
                                                                                       "At least 'LATITUDE' and 'LONGITUDE' must be given, 'TIME' is needed for <br/>" +
                                                                                       "application of the max. time difference criterion. The time information <br/>" +
                                                                                       "has be in the format 'yyyy-MM-dd HH:mm:ss'. Other names will be<br/>" +
                                                                                       "matched against names in the resulting L2 products in order to generate<br/>" +
                                                                                       "the match-up scatter-plots and statistics, for example 'CONC_CHL'." +
                                                                                       "<h4>Custom format:</h4>" +
                                                                                       "If the file deviates from the standard format header lines<br/>" +
                                                                                       "following the '# &lt;key&gt;=&lt;value&gt;' syntax can added to the file to customize the format:<br/>" +
                                                                                       "<ol>" +
                                                                                       "<li><b>columnSeparator</b> The character separating the columns.</li>" +
                                                                                       "<li><b>latColumn</b> The name of the column containing the latitude.</li>" +
                                                                                       "<li><b>lonColumn</b> The name of the column containing the longitude.</li>" +
                                                                                       "<li><b>timeColumn</b> The name of the column containing the time.</li>" +
                                                                                       "<li><b>timeColumns</b> A comma separated list of column names.<br/>" +
                                                                                       "The content will be concatenated separated by COMMA.</li>" +
                                                                                       "<li><b>dateFormat</b> The <a href=\"http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html\">dateformat</a> for parsing the time.</li>" +
                                                                                       "</ol>"));
            fileUploadDialog = new Dialog("File Upload", verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
                @Override
                protected void onOk() {
                    String filename = fileUpload.getFilename();
                    if (filename == null || filename.isEmpty()) {
                        Dialog.info("File Upload",
                                    new HTML("No filename selected."),
                                    new HTML("Please specify a point data file."));
                        return;
                    }
                    monitorDialog = new Dialog("File Upload", new Label("Submitting '" + filename + "'..."), ButtonType.CANCEL) {
                        @Override
                        protected void onCancel() {
                            cancelSubmit();
                        }
                    };
                    monitorDialog.show();
                    uploadForm.submit();
                }
            };

            fileUploadDialog.show();
        }

        private void cancelSubmit() {
            closeDialogs();
            if (submitEvent != null) {
                submitEvent.cancel();
            }
        }

        @Override
        public void onSubmit(FormPanel.SubmitEvent event) {
            this.submitEvent = event;
        }

        @Override
        public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
            closeDialogs();
            updateRecordSources();
            Dialog.info("File Upload",
                        "File successfully uploaded.");

        }

        private void closeDialogs() {
            monitorDialog.hide();
            fileUploadDialog.hide();
        }
    }

    private class RemoveRecordSourceAction implements ClickHandler {
        @Override
        public void onClick(ClickEvent event) {
            final String recordSource = getSelectedRecordSourceFilename();
            if (recordSource != null) {
                Dialog.ask("Remove File",
                           new HTML("The file '" + recordSource + "' will be permanently deleted.<br/>" +
                                            "Do you really want to continue?"),
                           new Runnable() {
                               @Override
                               public void run() {
                                   removeRecordSource(recordSource);
                               }
                           });
            } else {
                Dialog.error("Remove File",
                             "No file selected.");
            }
        }
    }

    private class CheckRecordSourceAction implements ClickHandler {
        @Override
        public void onClick(ClickEvent event) {
            final String recordSource = getSelectedRecordSourceFilename();
            // should be made async!
            checkRecordSource(recordSource);
        }
    }

    private class ViewRecordSourceAction implements ClickHandler {
        @Override
        public void onClick(ClickEvent event) {
            final String recordSource = getSelectedRecordSourceFilename();
            // should be made async!
            viewRecordSource(recordSource);
        }
    }

}
