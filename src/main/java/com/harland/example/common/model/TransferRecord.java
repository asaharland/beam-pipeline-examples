package com.harland.example.common.model;

import java.io.Serializable;

public class TransferRecord implements Serializable {

    private String user;
    private String transferTo;
    private Double amount;

    public TransferRecord(String user, String transferTo, Double amount) {
        this.user = user;
        this.transferTo = transferTo;
        this.amount = amount;
    }

    public String getUser() {
        return user;
    }

    public String getTransferTo() {
        return transferTo;
    }

    public Double getAmount() {
        return amount;
    }

}
