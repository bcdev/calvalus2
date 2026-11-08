package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class OGCLink {
    /** Required */
    public String href;

    public String rel;
    public String type;
    public String hreflang;
    public String title;

    public static OGCLink self(String href) {
        OGCLink link = new OGCLink();
        link.href = href;
        link.rel = "self";
        link.type = "application/json";
        return link;
    }
}
