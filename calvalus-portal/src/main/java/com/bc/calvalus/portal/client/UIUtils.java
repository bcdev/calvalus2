package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.datepicker.client.DateBox;

import java.util.Date;

/**
 * GUI utilities.
 * @author Norman
 */
public class UIUtils {
    public static void validateDateBox(String label, DateBox dateBox) throws ValidationException {
        Date value = dateBox.getValue();
        if (value == null) {
            throw new ValidationException(dateBox, "Missing value for '" + label + "'");
        }
    }

    public static void validateDoubleBox(String label, DoubleBox doubleBox, double min, double max) throws ValidationException {
        Double value = doubleBox.getValue();
        if (value == null) {
            throw new ValidationException(doubleBox, "Missing value for '" + label + "'");
        }
        if (value < min) {
            throw new ValidationException(doubleBox, "Value for '" + label + "' must be greater than or equal to " + min);
        }
        if (value > max) {
            throw new ValidationException(doubleBox, "Value for '" + label + "' must be greater than or equal to " + max);
        }
    }
}
