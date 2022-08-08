package com.product.data.publisher.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;


public class CustomerBalanceDetails implements Serializable {

    private String customerId;
    private String phoneNumber;
    private String accountId;
    private Float balance;

    public CustomerBalanceDetails(String customerId, String phoneNumber, String accountId, Float balance) {
        this.customerId = customerId;
        this.phoneNumber = phoneNumber;
        this.accountId = accountId;
        this.balance = balance;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Float getBalance() {
        return balance;
    }

    public void setBalance(Float balance) {
        this.balance = balance;
    }
}
