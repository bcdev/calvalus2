package com.bc.calvalus.processing.ma;

/**
 * Provides easy constructors for the test cases
 *
 * @author Marco Peters
 */
public class TestHeader extends DefaultHeader {

    public TestHeader(String... attributeNames) {
        this(false, attributeNames);
    }

    public TestHeader(boolean hasLocation, String... attributeNames) {
        super(hasLocation, false, attributeNames);
    }
}
