package org.example;

public class PossibleDropAlarm {
    public String investmentName;
    public Long timestamp;
    public String droppedStatName;

    public PossibleDropAlarm(String investmentName, Long timestamp, String droppedStatName) {
        this.investmentName = investmentName;
        this.timestamp = timestamp;
        this.droppedStatName = droppedStatName;
    }

    public String toString() {
        return "PossibleDropAlarm{" +
                "investmentName='" + investmentName + '\'' +
                ", timestamp=" + timestamp +
                ", droppedStatName='" + droppedStatName + '\'' +
                '}';
    }
}
