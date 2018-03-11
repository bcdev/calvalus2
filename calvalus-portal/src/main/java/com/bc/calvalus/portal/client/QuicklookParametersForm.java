package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;


/**
 * This form is used regarding visualisations settings.
 *
 * @author Declan
 */
public class QuicklookParametersForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, QuicklookParametersForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    CheckBox enableQuicklook;

    public QuicklookParametersForm(PortalContext portalContext) {
        initWidget(uiBinder.createAndBindUi(this));
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("enableQuicklook", enableQuicklook.getValue().toString());
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        String enableQuicklookValue = parameters.get("enableQuicklook");
        if (enableQuicklookValue != null) {
            enableQuicklook.setValue(Boolean.valueOf(enableQuicklookValue));
        }
    }
}