package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Marco
 * @author Norman
 */
public class ProcessorSelectionForm extends Composite {

    private GsProcessorDescriptor[] processorDescriptors;

    interface TheUiBinder extends UiBinder<Widget, ProcessorSelectionForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    Label label;
    @UiField
    ListBox processorList;

    public ProcessorSelectionForm(GsProcessorDescriptor[] processorDescriptors, String title) {
        this.processorDescriptors = processorDescriptors;
        initWidget(uiBinder.createAndBindUi(this));

        label.setText(title);
        for (GsProcessorDescriptor processor : processorDescriptors) {
            String label = processor.getBundleName() + "-" + processor.getBundleVersion();
            this.processorList.addItem(label);
        }
        if (processorDescriptors.length > 0) {
            processorList.setSelectedIndex(0);
        }
    }

    public GsProcessorDescriptor getSelectedProcessor() {
        int selectedIndex = processorList.getSelectedIndex();
        return processorDescriptors[selectedIndex];
    }

}