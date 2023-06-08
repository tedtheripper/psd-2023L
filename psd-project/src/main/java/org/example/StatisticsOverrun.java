package org.example;

public class StatisticsOverrun {
    public String investmentName;
    public Double averageOverrun;
    public Double quantileOverrun;
    public Double averageSmaller10PercentOverrun;
    public Long timestamp;

    public StatisticsOverrun(String investmentName, Double averageOverrun, Double quantileOverrun, Double averageSmaller10PercentOverrun, Long timestamp) {
        this.investmentName = investmentName;
        this.averageOverrun = averageOverrun;
        this.quantileOverrun = quantileOverrun;
        this.averageSmaller10PercentOverrun = averageSmaller10PercentOverrun;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "StatisticsOverrun{" +
                "investmentName='" + investmentName + '\'' +
                ", averageOverrun=" + averageOverrun +
                ", quantileOverrun=" + quantileOverrun +
                ", averageSmaller10PercentOverrun=" + averageSmaller10PercentOverrun +
                ", timestamp=" + timestamp +
                '}';
    }
}
