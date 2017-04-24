package com.bc.calvalus.urban.ws;

import lombok.Value;

/**
 * @author muhammad.bc.
 */
@Value
public class WpsReport {
    private String jobID;
    private String accRef;
    private String compID;
    private String status;
    private String hostName;
    private String uri;
    private String finishDateTime;

}
