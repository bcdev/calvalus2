package com.bc.calvalus.processing.ma;

/**
 * Various text utility functions.
 *
 * @author Norman
 */
public class TextUtils {

    public static int indexOf(String[] textValues, String possibleValue) {
        return indexOf(textValues, new String[] {possibleValue});
    }

    public static int indexOf(String[] textValues, String[] possibleValues) {
        for (String possibleValue : possibleValues) {
            for (int index = 0; index < textValues.length; index++) {
                if (possibleValue.equalsIgnoreCase(textValues[index])) {
                    return index;
                }
            }
        }
        return -1;
    }

}
