package com.bc.calvalus.processing.ma;

import java.text.DateFormat;
import java.text.ParseException;

/**
 * Various text utility functions.
 *
 * @author Norman
 */
public class TextUtils {

    public static int indexOf(String[] textValues, String[] possibleValues) {
        for (String possibleName : possibleValues) {
            for (int index = 0; index < textValues.length; index++) {
                if (possibleName.equalsIgnoreCase(textValues[index])) {
                    return index;
                }
            }
        }
        return -1;
    }

    public static Object[] convert(String[] textValues, DateFormat dateFormat) {
        final Object[] values = new Object[textValues.length];
        for (int i = 0; i < textValues.length; i++) {
            final String text = textValues[i];
            try {
                values[i] = Double.valueOf(text);
            } catch (NumberFormatException e) {
                try {
                    values[i] = dateFormat.parse(text);
                } catch (ParseException e1) {
                    values[i] = text;
                }
            }
        }
        return values;
    }
}
