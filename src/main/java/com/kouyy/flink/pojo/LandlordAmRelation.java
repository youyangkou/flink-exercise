package com.kouyy.flink.pojo;

public class LandlordAmRelation {
    String landlordId;
    String luid;
    String landlordName;
    String amId;

    public String getLuid() {
        return luid;
    }

    public void setLuid(String luid) {
        this.luid = luid;
    }

    public String getLandlordId() {
        return landlordId;
    }

    public void setLandlordId(String landlordId) {
        this.landlordId = landlordId;
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

    public LandlordAmRelation(String landlordId, String landlordName, String amId) {
        this.landlordId = landlordId;
        this.landlordName = landlordName;
        this.amId = amId;
    }
}
