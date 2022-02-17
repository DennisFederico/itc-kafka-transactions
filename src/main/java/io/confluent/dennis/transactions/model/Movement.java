package io.confluent.dennis.transactions.model;

public class Movement {
    String accountId;
    String txId;
    String type; //DEBIT or CREDIT
    double amount;

    public Movement() {
    }

    public Movement(String accountId, String txId, String type, double amount) {
        this.accountId = accountId;
        this.txId = txId;
        this.type = type;
        this.amount = amount;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Movement{" +
                "accountId='" + accountId + '\'' +
                ", txId='" + txId + '\'' +
                ", type='" + type + '\'' +
                ", amount=" + amount +
                '}';
    }
}
