package com.kouyy.flink.pojo;

//"bookOrder": {
//        "orderId": "70000186260200",
//        "parentOrderId": "70000186260200",
//        "luId": "70000155844503",
//        "submitterId": "26628735301",
//        "landlordId": "6582705415",
//        "lastState": null,
//        "currentState": "submitted",
//        "createTime": "2019-06-25 15:16:18",
//        "operatorId": "26628735301",
//        "autoConfirmOrder": false,
//        "dontDisturb": false,
//        "isPaid": false,
//        "checkInDate": "2019-06-30",
//        "checkOutDate": "2019-07-01"
//        }

public class BookOrder {

        private String orderId;
        private String parentOrderId;
        private String luId;
        private String submitterId;
        private String landlordId;
        private String lastState;
        private String currentState;
        private String createTime;
        private String operatorId;
        private Boolean autoConfirmOrder;
        private Boolean dontDisturb;
        private Boolean isPaid;
        private String checkInDate;
        private String checkOutDate;

    public String getAutoConfirmOrder() {
        if(autoConfirmOrder==null){
            return "";
        }
        return autoConfirmOrder.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setAutoConfirmOrder(Boolean autoConfirmOrder) {
        this.autoConfirmOrder = autoConfirmOrder;
    }

    public String getDontDisturb() {
        if(dontDisturb==null){
            return "";
        }
        return dontDisturb.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setDontDisturb(Boolean dontDisturb) {
        this.dontDisturb = dontDisturb;
    }

    public String getPaid() {
        if(isPaid==null){
            return "";
        }
        return isPaid.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setPaid(Boolean paid) {
        isPaid = paid;
    }

    @Override
    public String toString() {
        return getOrderId()+","+getParentOrderId()+","+getLuId()+","+getSubmitterId()+","+getLandlordId()+","+getLastState()+","+getCurrentState()+","+getCreateTime()
                +","+getOperatorId()+","+getAutoConfirmOrder()+","+getDontDisturb()+","+getPaid()+","+getCheckInDate()+","+getCheckOutDate();
    }

    public String getOrderId() {
        if(orderId==null){
            orderId="";
        }
        return orderId.replaceAll("[\b\r\n\t]*", "");
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getParentOrderId() {
        if(parentOrderId==null){
            parentOrderId="";
        }
        return parentOrderId.replaceAll("[\b\r\n\t]*", "");
    }

    public void setParentOrderId(String parentOrderId) {
        this.parentOrderId = parentOrderId;
    }

    public String getLuId() {
        if(luId==null){
            luId="";
        }
        return luId.replaceAll("[\b\r\n\t]*", "");
    }

    public void setLuId(String luId) {
        this.luId = luId;
    }

    public String getSubmitterId() {
        if(submitterId==null){
            submitterId="";
        }
        return submitterId.replaceAll("[\b\r\n\t]*", "");
    }

    public void setSubmitterId(String submitterId) {
        this.submitterId = submitterId;
    }

    public String getLandlordId() {
        if(landlordId==null){
            landlordId="";
        }
        return landlordId.replaceAll("[\b\r\n\t]*", "");
    }

    public void setLandlordId(String landlordId) {
        this.landlordId = landlordId;
    }

    public String getLastState() {
        if(lastState==null){
            lastState="";
        }
        return lastState.replaceAll("[\b\r\n\t]*", "");
    }

    public void setLastState(String lastState) {
        this.lastState = lastState;
    }

    public String getCurrentState() {
        if(currentState==null){
            currentState="";
        }
        return currentState.replaceAll("[\b\r\n\t]*", "");
    }

    public void setCurrentState(String currentState) {
        this.currentState = currentState;
    }

    public String getCreateTime() {
        if(createTime==null){
            createTime="";
        }
        return createTime.replaceAll("[\b\r\n\t]*", "");
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getOperatorId() {
        if(operatorId==null){
            operatorId="";
        }
        return operatorId.replaceAll("[\b\r\n\t]*", "");
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public String isAutoConfirmOrder() {
        if(autoConfirmOrder==null){
            return "";
        }
        return autoConfirmOrder.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setAutoConfirmOrder(boolean autoConfirmOrder) {
        this.autoConfirmOrder = autoConfirmOrder;
    }

    public String isDontDisturb() {
        if(dontDisturb==null){
            return "";
        }
        return dontDisturb.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setDontDisturb(boolean dontDisturb) {
        this.dontDisturb = dontDisturb;
    }

    public String isPaid() {
        if(isPaid==null){
            return "";
        }
        return isPaid.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setPaid(boolean paid) {
        isPaid = paid;
    }

    public String getCheckInDate() {
        if(checkInDate==null){
            checkInDate="";
        }
        return checkInDate.replaceAll("[\b\r\n\t]*", "");
    }

    public void setCheckInDate(String checkInDate) {
        this.checkInDate = checkInDate;
    }

    public String getCheckOutDate() {
        if(checkOutDate==null){
            checkOutDate="";
        }
        return checkOutDate.replaceAll("[\b\r\n\t]*", "");
    }

    public void setCheckOutDate(String checkOutDate) {
        this.checkOutDate = checkOutDate;
    }
}
