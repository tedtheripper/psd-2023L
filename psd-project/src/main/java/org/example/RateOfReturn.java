package org.example;

public class RateOfReturn {
    public String investmentName;
    public Float rate;
    public Long timestamp;

    public RateOfReturn() {}

    public RateOfReturn(String investmentName, Float rate, Long timestamp) {
        this.investmentName = investmentName;
        this.rate = rate;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "RateOfReturn{" +
                "investmentName='" + investmentName + '\'' +
                ", rate=" + rate +
                ", timestamp=" + timestamp +
                '}';
    }
}
