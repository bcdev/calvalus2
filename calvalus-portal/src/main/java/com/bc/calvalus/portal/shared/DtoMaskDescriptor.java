package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.processing.MaskDescriptor} class.
 *
 * @author Thomas
 */
public class DtoMaskDescriptor implements IsSerializable {


    private String maskName;
    private String descriptionHtml;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    @SuppressWarnings("unused")
    public DtoMaskDescriptor() {
    }

    public DtoMaskDescriptor(String maskName, String descriptionHtml) {
        this.maskName = maskName;
        this.descriptionHtml = descriptionHtml;
    }

    public String getMaskName() {
        return maskName;
    }

    public String getDescriptionHtml() {
        return descriptionHtml;
    }
}
