package com.bc.calvalus.urban.account;

import com.google.gson.Gson;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class Message {
    private String id;
    private Account account;
    private Compound compound;
    private List<Quantity> quantity;
    private String hostName;
    private String timeStamp;
    private String status;

    public Message(String id, Account account, Compound compound, List<Quantity> quantity, String hostName, String timeStamp, String status) {
        this.id = id;
        this.account = account;
        this.compound = compound;
        this.quantity = quantity;
        this.hostName = hostName;
        this.timeStamp = timeStamp;
        this.status = status;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public String getId() {
        return id;
    }

    public Account getAccount() {
        return account;
    }

    public Compound getCompound() {
        return compound;
    }

    public List<Quantity> getQuantity() {
        return quantity;
    }

    public String getHostName() {
        return hostName;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getStatus() {
        return status;
    }
}
