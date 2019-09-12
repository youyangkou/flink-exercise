package com.kouyy.flink.pojo;

/**
 * @author kouyouyang
 * @date 2019-08-13 18:34
 */
public class GioKafkaEvent {

    private String id;
    private Long eventTime;
    private String info;

    public GioKafkaEvent() {
    }

    public GioKafkaEvent(String id, Long eventTime, String info) {
        this.id = id;
        this.eventTime = eventTime;
        this.info = info;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public boolean hasWatermarkMarker(){
        return eventTime !=null;
    }
}
