package kafka.examples.producer;


import java.io.Serializable;

public class MyEvent implements Serializable {

    int eventId;
    String name;
    String eventType;
    long eventTime;

    public MyEvent(int eventId, String name, String eventType, long eventTime) {
        this.eventId = eventId;
        this.name = name;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public int getEventId() {
        return eventId;
    }

    public String getName() {
        return name;
    }

    public String getEventType() {
        return eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public void setEventId(int eventId) {
        this.eventId = eventId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "eventId='" + eventId + '\'' +
                ", name='" + name + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
