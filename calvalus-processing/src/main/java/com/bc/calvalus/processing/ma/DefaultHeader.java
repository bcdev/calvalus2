package com.bc.calvalus.processing.ma;

import java.util.Arrays;
import java.util.List;

/**
 * A default implementation of a {@link Header}.
 *
 * @author Norman
 */
public class DefaultHeader implements Header {

    public static final String ANNOTATION_EXCLUSION_REASON = "ExclusionReason";

    private final boolean hasLocation;
    private final boolean hasTime;
    private final List<String> attributeNames;
    private final List<String> annotationNames;

    public DefaultHeader(boolean hasLocation, boolean hasTime, String... attributeNames) {
        this(hasLocation, hasTime, attributeNames, new String[]{ANNOTATION_EXCLUSION_REASON});
    }

    private DefaultHeader(boolean hasLocation, boolean hasTime, String[] attributeNames, String[] annotationNames) {
        this.hasLocation = hasLocation;
        this.hasTime = hasTime;
        this.attributeNames = Arrays.asList(attributeNames);
        this.annotationNames = Arrays.asList(annotationNames);
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
    public int getAttributeIndex(String name) {
        return attributeNames.indexOf(name);
    }

    @Override
    public String[] getAnnotationNames() {
        return annotationNames.toArray(new String[annotationNames.size()]);
    }

    @Override
    public int getAnnotationIndex(String name) {
        return annotationNames.indexOf(name);
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
