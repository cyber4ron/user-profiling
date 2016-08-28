package com.lianjia.profiling.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * delegation.json event & need info. for es doc reading.
 *
 * @author fenglei@wandoujia.com on 16/3/7.
 */

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class DelegationEvent {
    private String custId;

    private String brokerCode;

    private String createTime;

    private String phone;

    private int bizType;

    private String district;

    private String bizCircle;

    private int roomMin;

    private int roomMax;

    private int areaMin;

    private int areaMax;

    private int needBalcony; // todo 0， 1，-1，语义

    @JsonGetter("cust_id")
    public String getCustId() {
        return custId;
    }

    public void setCustId(String custId) {
        this.custId = custId;
    }

    @JsonGetter("broker_id")
    public String getBrokerCode() {
        return brokerCode;
    }

    public void setBrokerCode(String brokerCode) {
        this.brokerCode = brokerCode;
    }

    @JsonGetter("created_time")
    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @JsonGetter("biz_type")
    public int getBizType() {
        return bizType;
    }

    public void setBizType(int bizType) {
        this.bizType = bizType;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    @JsonGetter("biz_circle")
    public String getBizCircle() {
        return bizCircle;
    }

    public void setBizCircle(String bizCircle) {
        this.bizCircle = bizCircle;
    }

    @JsonGetter("room_min")
    public int getRoomMin() {
        return roomMin;
    }

    public void setRoomMin(int roomMin) {
        this.roomMin = roomMin;
    }

    @JsonGetter("room_max")
    public int getRoomMax() {
        return roomMax;
    }

    public void setRoomMax(int roomMax) {
        this.roomMax = roomMax;
    }

    @JsonGetter("area_max")
    public int getAreaMax() {
        return areaMax;
    }

    public void setAreaMax(int areaMax) {
        this.areaMax = areaMax;
    }

    @JsonGetter("area_min")
    public int getAreaMin() {
        return areaMin;
    }

    public void setAreaMin(int areaMin) {
        this.areaMin = areaMin;
    }

    @JsonGetter("need_balcony")
    public int getNeedBalcony() {
        return needBalcony;
    }

    public void setNeedBalcony(int needBalcony) {
        this.needBalcony = needBalcony;
    }
}
