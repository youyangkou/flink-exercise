package com.kouyy.flink.pojo;


public class OrderMessage {

        private String orderState;
        private String objId;
        private Content content;
        private Long eventTime;

        public void setOrderState(String orderState) {
            this.orderState = orderState;
        }
        public String getOrderState() {
            if(orderState==null){
                return "";
            }
            return orderState.replaceAll("[\b\r\n\t]*", "");
        }

        public void setObjId(String objId) {
            this.objId = objId;
        }
        public String getObjId() {
            if(objId==null){
                return "";
            }
            return objId.replaceAll("[\b\r\n\t]*", "");
        }

        public void setContent(Content content) {
            this.content = content;
        }

        public Content getContent() {
            return content;
        }

        //Content字符串
    public String getContentString() {
        if(content==null){
            return "";
        }
        BookOrder bookOrder=content.getBookOrder();
        if(bookOrder==null){
            return "";
        }else{
            return bookOrder.toString();
        }
    }

        public void setEventTime(long eventTime) {
            this.eventTime = eventTime;
        }

        public String getEventTime() {
            if(eventTime==null){
                return "";
            }
            return eventTime.toString().replaceAll("[\b\r\n\t]*", "");
        }

    @Override
    public String toString() {
            //getCs1()+","+getB() +","+getD()+","+getE()+","+getX()+","+getTm()+","+getV()+","+getIdx()+","+getCstm()+","+getPtm()+","+getUa()+","+getUri()+","+getStm()+","+getP()+","+getQ()+","+getS()+","+getT()+","+getU()+"\n"
        return getOrderState()+","+getObjId()+","+getContentString()+","+getEventTime()+"\n";
    }


    public String to_String() {
        return "OrderMessage{" +
                "orderState='" + orderState + '\'' +
                ", objId='" + objId + '\'' +
                ", content=" + content +
                ", eventTime=" + eventTime +
                '}';
    }
}


