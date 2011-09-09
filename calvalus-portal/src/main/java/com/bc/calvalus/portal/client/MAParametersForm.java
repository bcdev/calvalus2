package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Form for MA (matup analysis) parameters.
 *
 * @author Norman
 */
public class MAParametersForm extends Composite {

    private final Map<String, DtoProcessorVariable> processorVariableDefaults;

    interface TheUiBinder extends UiBinder<Widget, MAParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox recordSources;
    @UiField
    Button addRecordSourceButton;
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

    public MAParametersForm(final PortalContext portalContext) {
        processorVariableDefaults = new HashMap<String, DtoProcessorVariable>();

        initWidget(uiBinder.createAndBindUi(this));


        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        uploadForm = new FormPanel();
        uploadForm.setWidget(fileUpload);

        FileUploadManager.configureForm(uploadForm,
                                        "echo=1",
                                        new FormPanel.SubmitHandler() {
                                            public void onSubmit(FormPanel.SubmitEvent event) {
                                                // we can check for valid input here
                                            }
                                        },
                                        new FormPanel.SubmitCompleteHandler() {
                                            public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                                                String results = event.getResults();
                                                System.out.println("results = " + results);
                                                updateRecordSources(portalContext);
                                                Dialog.showMessage("File Upload",
                                                                   new Label("File successfully uploaded."));

                                            }
                                        }
        );

        addRecordSourceButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                VerticalPanel verticalPanel = UIUtils.createVerticalPanel(2,
                                                                          new HTML("Select in-situ or point data file:"),
                                                                          uploadForm,
                                                                          new HTML("The supported file types are TAB-separated CSV (<b>*.txt</b>, <b>*.csv</b>) and BEAM placemark files (<b>*.placemark</b>)."));
                                                                          new HTML("The first line of the TAB-separated CSV file must contain header names, e.g. LAT, LON, TIME, CONC_CHL.");
                Dialog dialog = new Dialog("File Upload", verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
                    @Override
                    protected void onOk() {
                        String filename = fileUpload.getFilename();
                        if (filename == null || filename.isEmpty()) {
                            Dialog.showMessage("File Upload",
                                               new HTML("No filename selected."),
                                               new HTML("Please specify a in-situ or point data file."),
                                               new HTML("Supported are TAB-separated CSV (<b>*.txt</b>, <b>*.csv</b>) files"),
                                               new HTML("and BEAM placemark files (<b>*.placemark</b>)."));
                            return;
                        }
                        uploadForm.submit();
                        hide();
                    }
                };
                dialog.show();
            }
        });

        removeRecordSourceButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                // todo - remove selected file from server
                Dialog.showMessage("File Upload",
                                   new HTML("Not yet implemented."));
            }
        });

        macroPixelSize.setValue(5);
        maxTimeDifference.setValue(3.0);
        filteredMeanCoeff.setValue(1.5);
        outputGroupName.setValue("SITE");

        updateRecordSources(portalContext);
    }

    private void updateRecordSources(PortalContext portalContext) {
        portalContext.getBackendService().listUserFiles("", new AsyncCallback<String[]>() {
            @Override
            public void onSuccess(String[] filePaths) {
                setRecordSources(filePaths);
            }

            @Override
            public void onFailure(Throwable caught) {
                // todo
            }
        });
    }

    private void setRecordSources(String[] filePaths) {
        recordSources.clear();
        final String BASE_DIR = "calvalus/home/";
        for (String filePath : filePaths) {
            int baseDirPos = filePath.indexOf(BASE_DIR);
            if (baseDirPos >= 0) {
                recordSources.addItem(filePath.substring(baseDirPos + BASE_DIR.length()), filePath);
            } else {
                recordSources.addItem(filePath, filePath);
            }
        }
        if (recordSources.getItemCount() > 0) {
            recordSources.setSelectedIndex(0);
        }
    }

    public void setSelectedProcessor(DtoProcessorDescriptor selectedProcessor) {
        if (selectedProcessor == null) {
            return;
        }
        processorVariableDefaults.clear();
        goodPixelExpression.setValue(selectedProcessor.getDefaultMaskExpr());
        goodRecordExpression.setValue(selectedProcessor.getProcessorVariables()[0].getName() + ".cv < 0.15");
    }

    public void validateForm() throws ValidationException {
        boolean macroPixelSizeValid = macroPixelSize.getValue() >= 1
                && macroPixelSize.getValue() <= 31
                && macroPixelSize.getValue() % 2 == 1;
        if (!macroPixelSizeValid) {
            throw new ValidationException(macroPixelSize, "Macro pixel size must be an odd integer between 1 and 31");
        }
        boolean filteredMeanCoeffValid = filteredMeanCoeff.getValue() > 0;
        if (!filteredMeanCoeffValid) {
            throw new ValidationException(filteredMeanCoeff, "Filtered mean coefficient must be >= 1");
        }

        boolean maxTimeDifferenceValid = maxTimeDifference.getValue() >= 0;
        if (!maxTimeDifferenceValid) {
            throw new ValidationException(maxTimeDifference, "Max. time difference must be >= 0 hours (0 disables time criterion)");
        }

        boolean outputGroupNameValid = !outputGroupName.getText().trim().isEmpty();
        if (!outputGroupNameValid) {
            throw new ValidationException(maxTimeDifference, "Output group name must be given.");
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


}