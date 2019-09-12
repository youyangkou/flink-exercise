package com.kouyy.flink.pojo;

public class AlertMessage {
    String landlordId;
    String tenantId;
    String landlordName;
    String orderId;
    String luId;
    String alertType;
    String amId;

    public AlertMessage() {
    }

    public AlertMessage(String landlordId, String tenantId, String landlordName, String orderId, String luId, String alertType, String amId) {
        this.landlordId = landlordId;
        this.tenantId = tenantId;
        this.landlordName = landlordName;
        this.orderId = orderId;
        this.luId = luId;
        this.alertType = alertType;
        this.amId = amId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getLandlordId() {
        return landlordId;
    }

    public void setLandlordId(String landlordId) {
        this.landlordId = landlordId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getLuId() {
        return luId;
    }

    public void setLuId(String luId) {
        this.luId = luId;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getLandlordName() {
        return landlordName;
    }

    public void setLandlordName(String landlordName) {
        this.landlordName = landlordName;
    }

    public String getAmId() {
        return amId;
    }

    public void setAmId(String amId) {
        this.amId = amId;
    }

    @Override
    public String toString() {
        return "AlertMessage{" +
                "landlordId='" + landlordId + '\'' +
                ", landlordName='" + landlordName + '\'' +
                ", orderId='" + orderId + '\'' +
                ", luId='" + luId + '\'' +
                ", alertType='" + alertType + '\'' +
                ", amId='" + amId + '\'' +
                '}';
    }
}
