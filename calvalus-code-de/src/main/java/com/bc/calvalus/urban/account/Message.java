package com.bc.calvalus.urban.account;

import java.util.Map;
import lombok.Value;

/**
 * @author muhammad.bc.
 */
@Value
public class Message {
    private String jobID;
    private Account account;
    private Compound compound;
    private Map<String, Long> quantity;
    private String hostName;
    private String timeStamp;
    private String status;
}
