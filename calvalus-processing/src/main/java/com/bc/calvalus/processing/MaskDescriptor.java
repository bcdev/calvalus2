package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.esa.snap.core.gpf.annotations.Parameter;

/**
 * Class containing information about a mask.
 */
public class MaskDescriptor {

    @Parameter
    private String maskName;

    // Short description in XHTML
    @Parameter
    String maskDescriptionHTML;

    private String maskLocation;
    private String owner;

    public MaskDescriptor() {
    }

    public MaskDescriptor(String maskName, String maskDescription, String maskLocation) {
        Assert.notNull(maskName, "maskName");
        this.maskName = maskName;
        this.maskDescriptionHTML = maskDescription;
        this.maskLocation = maskLocation;
    }

    public void setMaskName(String maskName) {
        this.maskName = maskName;
    }

    public void setMaskDescriptionHTML(String maskDescriptionHTML) {
        this.maskDescriptionHTML = maskDescriptionHTML;
    }

    public void setMaskLocation(String maskLocation) {
        this.maskLocation = maskLocation;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getMaskName() {
        return maskName;
    }

    public String getMaskDescriptionHTML() {
        return maskDescriptionHTML;
    }

    public String getMaskLocation() {
        return maskLocation;
    }

    public String getOwner() {
        return owner == null ? "" : owner;
    }

}
