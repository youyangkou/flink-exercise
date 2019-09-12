package com.kouyy.flink.pojo;

import java.util.Objects;

public class IMmessage {
//    {
//            "imTalkMsgId": 109580907300,
//            "antiSpam": null,
//            "fromUserId": "6178674215",
//            "fromUserClientType": "android",
//            "toUserId": "10096468159",
//            "localId": "e2ae49ca-91ed-42c2-9ac1-bbb3fba2ba78",
//            "luId": "31751571203",
//            "landlordId": "10096468159",
//            "storeContent": "最起码得有个3居室的",
//            "pluginType": "chat",
//            "fromUserIdUserRole": "tenant",
//            "roomName": "北戴河鸽子窝附近豪华四室两厅双卫可做饭"
//    }
    Long  imTalkMsgId;
    String antiSpam;
    String fromUserId;
    String fromUserClientType;
    String toUserId;
    String localId;
    String luId;
    String landlordId;
    String storeContent;
    String pluginType;
    String fromUserIdUserRole;
    String roomName;

    public IMmessage(Long imTalkMsgId, String antiSpam, String fromUserId, String fromUserClientType, String toUserId, String localId, String luId, String landlordId, String storeContent, String pluginType, String fromUserIdUserRole, String roomName) {
        this.imTalkMsgId = imTalkMsgId;
        this.antiSpam = antiSpam;
        this.fromUserId = fromUserId;
        this.fromUserClientType = fromUserClientType;
        this.toUserId = toUserId;
        this.localId = localId;
        this.luId = luId;
        this.landlordId = landlordId;
        this.storeContent = storeContent;
        this.pluginType = pluginType;
        this.fromUserIdUserRole = fromUserIdUserRole;
        this.roomName = roomName;
    }

    public IMmessage() {
    }

    public Long getImTalkMsgId() {
        return imTalkMsgId;
    }

    public void setImTalkMsgId(Long imTalkMsgId) {
        this.imTalkMsgId = imTalkMsgId;
    }

    public String getAntiSpam() {
        return antiSpam;
    }

    public void setAntiSpam(String antiSpam) {
        this.antiSpam = antiSpam;
    }

    public String getFromUserId() {
        return fromUserId;
    }

    public void setFromUserId(String fromUserId) {
        this.fromUserId = fromUserId;
    }

    public String getFromUserClientType() {
        return fromUserClientType;
    }

    public void setFromUserClientType(String fromUserClientType) {
        this.fromUserClientType = fromUserClientType;
    }

    public String getToUserId() {
        return toUserId;
    }

    public void setToUserId(String toUserId) {
        this.toUserId = toUserId;
    }

    public String getLocalId() {
        return localId;
    }

    public void setLocalId(String localId) {
        this.localId = localId;
    }

    public String getLuId() {
        return luId;
    }

    public void setLuId(String luId) {
        this.luId = luId;
    }

    public String getLandlordId() {
        return landlordId;
    }

    public void setLandlordId(String landlordId) {
        this.landlordId = landlordId;
    }

    public String getStoreContent() {
        return storeContent;
    }

    public void setStoreContent(String storeContent) {
        this.storeContent = storeContent;
    }

    public String getPluginType() {
        return pluginType;
    }

    public void setPluginType(String pluginType) {
        this.pluginType = pluginType;
    }

    public String getFromUserIdUserRole() {
        return fromUserIdUserRole;
    }

    public void setFromUserIdUserRole(String fromUserIdUserRole) {
        this.fromUserIdUserRole = fromUserIdUserRole;
    }

    public String getRoomName() {
        return roomName;
    }

    public void setRoomName(String roomName) {
        this.roomName = roomName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IMmessage)) return false;
        IMmessage iMmessage = (IMmessage) o;
        return  Objects.equals(getImTalkMsgId(), iMmessage.getImTalkMsgId()) &&
                Objects.equals(getAntiSpam(), iMmessage.getAntiSpam()) &&
                Objects.equals(getFromUserId(), iMmessage.getFromUserId()) &&
                Objects.equals(getFromUserClientType(), iMmessage.getFromUserClientType()) &&
                Objects.equals(getToUserId(), iMmessage.getToUserId()) &&
                Objects.equals(getLocalId(), iMmessage.getLocalId()) &&
                Objects.equals(getLuId(), iMmessage.getLuId()) &&
                Objects.equals(getLandlordId(), iMmessage.getLandlordId()) &&
                Objects.equals(getStoreContent(), iMmessage.getStoreContent()) &&
                Objects.equals(getPluginType(), iMmessage.getPluginType()) &&
                Objects.equals(getFromUserIdUserRole(), iMmessage.getFromUserIdUserRole()) &&
                Objects.equals(getRoomName(), iMmessage.getRoomName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getImTalkMsgId(), getAntiSpam(), getFromUserId(), getFromUserClientType(), getToUserId(), getLocalId(), getLuId(), getLandlordId(), getStoreContent(), getPluginType(), getFromUserIdUserRole(), getRoomName());
    }

    @Override
    public String toString() {
        return "IMmessage{" +
                "imTalkMsgId=" + imTalkMsgId +
                ", antiSpam='" + antiSpam + '\'' +
                ", fromUserId='" + fromUserId + '\'' +
                ", fromUserClientType='" + fromUserClientType + '\'' +
                ", toUserId='" + toUserId + '\'' +
                ", localId='" + localId + '\'' +
                ", luId='" + luId + '\'' +
                ", landlordId='" + landlordId + '\'' +
                ", storeContent='" + storeContent + '\'' +
                ", pluginType='" + pluginType + '\'' +
                ", fromUserIdUserRole='" + fromUserIdUserRole + '\'' +
                ", roomName='" + roomName + '\'' +
                '}';
    }
}
