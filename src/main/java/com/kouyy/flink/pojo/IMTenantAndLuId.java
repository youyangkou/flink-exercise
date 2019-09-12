package com.kouyy.flink.pojo;

public class IMTenantAndLuId {
    String tenantId_luid;
    String tenantId;
    String storeContent;
    String fromOrTo;

    public IMTenantAndLuId(String tenantId_luid, String tenantId, String storeContent, String fromOrTo) {
        this.tenantId_luid = tenantId_luid;
        this.tenantId = tenantId;
        this.storeContent = storeContent;
        this.fromOrTo = fromOrTo;
    }

    public IMTenantAndLuId() {
    }

    public String getFromOrTo() {
        return fromOrTo;
    }

    public void setFromOrTo(String fromOrTo) {
        this.fromOrTo = fromOrTo;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTenantId_luid() {
        return tenantId_luid;
    }

    public void setTenantId_luid(String imTalkMsgId_tenantId_luid) {
        this.tenantId_luid = imTalkMsgId_tenantId_luid;
    }

    public String getStoreContent() {
        return storeContent;
    }

    public void setStoreContent(String storeContent) {
        this.storeContent = storeContent;
    }

    @Override
    public String toString() {
        return "IMTenantAndLuId{" +
                "tenantId_luid='" + tenantId_luid + '\'' +
                ", tenantId='" + tenantId + '\'' +
                ", storeContent='" + storeContent + '\'' +
                ", fromOrTo='" + fromOrTo + '\'' +
                '}';
    }
}
