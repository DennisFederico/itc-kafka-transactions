package io.confluent.dennis.transactions.model;

public class Transaction {

    String txId;
    String firstPartyId;
    String secondPartyId;
    double amount;

    public Transaction() {

    }

    public Transaction(String txId, String firstPartyId, String secondPartyId, double amount) {
        this.txId = txId;
        this.firstPartyId = firstPartyId;
        this.secondPartyId = secondPartyId;
        this.amount = amount;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getFirstPartyId() {
        return firstPartyId;
    }

    public void setFirstPartyId(String firstPartyId) {
        this.firstPartyId = firstPartyId;
    }

    public String getSecondPartyId() {
        return secondPartyId;
    }

    public void setSecondPartyId(String secondPartyId) {
        this.secondPartyId = secondPartyId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txId='" + txId + '\'' +
                ", firstPartyId='" + firstPartyId + '\'' +
                ", secondPartyId='" + secondPartyId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
