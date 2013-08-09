package com.bc.calvalus.processing.ma;

import java.util.Arrays;
import java.util.List;

/**
 * A default implementation of a {@link Header}.
 *
 * @author Norman
 */
public class DefaultHeader implements Header {

    private final boolean hasLocation;
    private final boolean hasTime;
    private final List<String> attributeNames;

    public DefaultHeader(String... attributeNames) {
        this(false, false, attributeNames);
    }

    public DefaultHeader(boolean hasLocation, String... attributeNames) {
        this(hasLocation, false, attributeNames);
    }

    public DefaultHeader(boolean hasLocation, boolean hasTime, String... attributeNames) {
        this.hasLocation = hasLocation;
        this.hasTime = hasTime;
        this.attributeNames = Arrays.asList(attributeNames);
    }

    @Override
    public boolean hasLocation() {
        return hasLocation;
    }

    @Override
    public boolean hasTime() {
        return hasTime;
    }

    @Override
    public String[] getAttributeNames() {
        return attributeNames.toArray(new String[attributeNames.size()]);
    }

    @Override
    public String getAttributeName(int index) {
        if (index >= 0) {
            return attributeNames.get(index);
        } else {
            return null;
        }
    }
}
