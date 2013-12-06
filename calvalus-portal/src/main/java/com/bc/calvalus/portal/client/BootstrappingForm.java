package com.bc.calvalus.portal.client;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.production.hadoop.ProcessorProductionRequest;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marco Peters
 */
public class BootstrappingForm extends Composite {

    private static final int DEFAULT_NUMBER_OF_ITERATIONS = 10000;
    private final UserManagedFiles userManagedContent;
    private final PortalContext portalContext;


    interface TheUiBinder extends UiBinder<Widget, BootstrappingForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox bootstrapSources;
    @UiField
    Button addBootstrapSourceButton;
    @UiField
    Button removeBootstrapSourceButton;

    @UiField
    ListBox processorList;
    @UiField
    IntegerBox numberOfIterations;
    @UiField
    TextBox productionName;

    private final List<DtoProcessorDescriptor> processorDescriptors;
    private final Filter<DtoProcessorDescriptor> processorFilter;


    public BootstrappingForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        processorDescriptors = new ArrayList<DtoProcessorDescriptor>();
        processorFilter = new Filter<DtoProcessorDescriptor>() {
            @Override
            public boolean accept(DtoProcessorDescriptor dtoProcessorDescriptor) {
                return true;
            }
        };

        numberOfIterations.setValue(DEFAULT_NUMBER_OF_ITERATIONS);

        final String fileExtension = ".csv";
        final String baseDir = "bootstrapping";
        HTML description = new HTML("The supported file types are TAB-separated CSV (<b>*" + fileExtension + "</b>) matchup files.<br/>");
        userManagedContent = new UserManagedFiles(portalContext.getBackendService(),
                                                  bootstrapSources,
                                                  baseDir,
                                                  "matchup",
                                                  description);
        userManagedContent.setFilePathFilter(new Filter<String>() {
            @Override
            public boolean accept(String filePath) {
                int lastSlashIndex = filePath.lastIndexOf("/");
                boolean inBaseDir = false;
                if (lastSlashIndex >= baseDir.length()) {
                    String substring = filePath.substring(lastSlashIndex - baseDir.length(), lastSlashIndex);
                    inBaseDir = baseDir.equals(substring);
                }
                return filePath.endsWith(fileExtension) && inBaseDir;
            }
        });
        addBootstrapSourceButton.addClickHandler(userManagedContent.getAddAction());
        removeBootstrapSourceButton.addClickHandler(userManagedContent.getRemoveAction());
        userManagedContent.updateList();

        //TODO filter bootstrap processor(s) in all other views
        updateProcessorList();
    }

    public void validateForm() throws ValidationException {

        Integer numberOfIterationsValue = numberOfIterations.getValue();
        if (numberOfIterationsValue == null || numberOfIterationsValue <= 0) {
            throw new ValidationException(numberOfIterations, "Number of Iterations must be > 0");
        }

        boolean bootstrapSourceValid = bootstrapSources.getSelectedIndex() >= 0;
        if (!bootstrapSourceValid) {
            throw new ValidationException(bootstrapSources, "Bootstrap source must be given.");
        }
        boolean processorDescriptorValid = getSelectedProcessorDescriptor() != null;
        if (!processorDescriptorValid) {
            throw new ValidationException(this, "No Bootstrapping processor selected.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        DtoProcessorDescriptor processorDescriptor = getSelectedProcessorDescriptor();
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, processorDescriptor.getBundleName());
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, processorDescriptor.getBundleVersion());
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, processorDescriptor.getBundleLocation());
        parameters.put(ProcessorProductionRequest.PROCESSOR_NAME, processorDescriptor.getProcessorName());

        parameters.put("calvalus.bootstrap.numberOfIterations", numberOfIterations.getText());
        parameters.put("calvalus.bootstrap.inputFile", userManagedContent.getSelectedFilename());
        parameters.put("productionName", productionName.getValue());
        return parameters;
    }

    public void updateProcessorList() {
        DtoProcessorDescriptor oldSelection = getSelectedProcessorDescriptor();

        processorDescriptors.clear();
        Collections.addAll(processorDescriptors, portalContext.getProcessors(BundleFilter.PROVIDER_USER));

        final Iterator<DtoProcessorDescriptor> iterator = processorDescriptors.iterator();
        while (iterator.hasNext()) {
            DtoProcessorDescriptor processorDescriptor = iterator.next();
            if (!"Bootstrapping in R".equals(processorDescriptor.getProcessorName())) {
                iterator.remove();
            }
        }

        processorList.clear();
        int newSelectionIndex = 0;
        boolean productSetChanged = true;
        for (DtoProcessorDescriptor processor : processorDescriptors) {
            String label = processor.getProcessorName() + " v" + processor.getProcessorVersion();
            processorList.addItem(label);
            if (oldSelection != null && oldSelection.equals(processor)) {
                newSelectionIndex = processorList.getItemCount() - 1;
                productSetChanged = false;
            }
        }
        processorList.setSelectedIndex(newSelectionIndex);
        if (productSetChanged) {
            DomEvent.fireNativeEvent(Document.get().createChangeEvent(), processorList);
        }
    }

    public DtoProcessorDescriptor getSelectedProcessorDescriptor() {
        int selectedIndex = processorList.getSelectedIndex();
        if (selectedIndex >= 0) {
            return processorDescriptors.get(selectedIndex);
        } else {
            return null;
        }
    }

}
