package com.kouyy.flink.pojo;

/**
 * 邮件提醒类型
 */
public  enum AlertTypeEnum {

        OrderConfirmTimeoutAlert("房东确认订单超时"),

        LandlordCancelAlert("房东拒单"),

        OrderNoRoomAlert("房客取消选择无房"),

        ImNoRoomAlert("IM触发无房");

        private String name;

        AlertTypeEnum(String name) {
                this.name = name;
        }

        public String getName() {
                return this.name;
        }

}

