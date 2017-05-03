package com.bc.calvalus.urban.account;

import com.google.gson.Gson;
import java.util.List;
import lombok.Value;

/**
 * @author muhammad.bc.
 */
@Value
public class Message {
    private String jobID;
    private Account account;
    private Compound compound;
    private List<Quantity> quantity;
    private String hostName;
    private String timeStamp;
    private String status;

    public String toJson() {
        return new Gson().toJson(this);
    }
}
