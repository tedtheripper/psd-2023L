package org.example;

public class PossibleDropAlarm {
    public String investmentName;
    public String droppedStatName;

    public PossibleDropAlarm(String investmentName, String droppedStatName) {
        this.investmentName = investmentName;
        this.droppedStatName = droppedStatName;
    }

    public String toString() {
        return "DropAlarm{" +
                "investmentName='" + investmentName + '\'' +
                ", droppedStatName=" + droppedStatName +
                '}';
    }
}
