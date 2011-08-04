package com.bc.calvalus.processing.ma;

/**
 * A default implementation of a {@link RecordSource}.
 * Its main purpose is testing.
 *
 * @author Norman
 */
public class DefaultHeader implements Header {

    private final String[] attributeNames;

    public DefaultHeader(String... attributeNames) {
        this.attributeNames = attributeNames;
    }

    @Override
    public String[] getAttributeNames() {
        return attributeNames.clone();
    }
}
