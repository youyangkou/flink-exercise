package com.kouyy.flink.pojo;

import com.kouyy.flink.utils.StringUtil;

import java.util.List;
import java.util.Map;

/**
 * rabbitmq中的搜索日志
 */
public class Search {

    private Map<String,String> city;
    private Extend extend;
    private Bookables bookables;
    private Days days;
    private Lodgeunittags lodgeunittags;
    private Latlng latlng;
    private Price price;
    private Leasetype leasetype;
    private Shower shower;
    private Aircondition aircondition;
    private Tv tv;
    private Housetyperoomcnt housetyperoomcnt;
    private Sort sort;
    private Guestnum guestnum;
    private Page page;
    private OccurTime occurTime;

    public String getOccurTime() {
        if(occurTime==null){
            return "";
        }
        return occurTime.toString().replaceAll("[\b\r\n\t]*", "");
    }

    public void setOccurTime(OccurTime occurTime) {
        this.occurTime = occurTime;
    }

    @Override
    public String toString() {
        String userId="";
        String userUniqueId="";
        if(getExtend()!=null){
            if(getExtend().getValue()!=null){
                if(!StringUtil.isEmptyOrWhiteSpace(getExtend().getValue().get("userId"))){
                    userId=getExtend().getValue().get("userId");
                }
                if(!StringUtil.isEmptyOrWhiteSpace(getExtend().getValue().get("userUniqueId"))){
                    userUniqueId=getExtend().getValue().get("userUniqueId");
                }
            }
        }
        return userId+"\t"+userUniqueId+"\t"+getCity()+"\t"+getBookables()+"\t"+getOccurTime()+"\n";
    }

    public String getCity() {
        if(city==null){
            return "";
        }
        if(StringUtil.isEmptyOrWhiteSpace(city.get("value"))){
            return "";
        }
        return city.get("value");
    }

    public void setCity(Map<String, String> city) {
        this.city = city;
    }

    public Extend getExtend() {
        return extend;
    }

    public void setExtend(Extend extend) {
        this.extend = extend;
    }

    public String getBookables() {
        if(bookables==null){
            return "";
        }
        if(bookables.getValue()==null||bookables.getValue().size()==0){
            return "";
        }
//        String value="";
//        for (Integer integer : bookables.getValue()) {
//            value=value+integer.toString()+"&";
//        }
//        if(value.length()!=0){
//            value.substring(0, value.length() - 1);
//        }
        StringBuffer sb = new StringBuffer();
        if(bookables.getValue().size()>0){
            for (Integer integer : bookables.getValue()) {
                sb.append(integer.toString());
                sb.append("&");
            }
        }
        String res ="";
        if(sb.length()>=1){
            res = sb.substring(0, sb.length()- 1);
        }
        return res;
    }

    public void setBookables(Bookables bookables) {
        this.bookables = bookables;
    }

    public Days getDays() {
        return days;
    }

    public void setDays(Days days) {
        this.days = days;
    }

    public Lodgeunittags getLodgeunittags() {
        return lodgeunittags;
    }

    public void setLodgeunittags(Lodgeunittags lodgeunittags) {
        this.lodgeunittags = lodgeunittags;
    }

    public Latlng getLatlng() {
        return latlng;
    }

    public void setLatlng(Latlng latlng) {
        this.latlng = latlng;
    }

    public Price getPrice() {
        return price;
    }

    public void setPrice(Price price) {
        this.price = price;
    }

    public Leasetype getLeasetype() {
        return leasetype;
    }

    public void setLeasetype(Leasetype leasetype) {
        this.leasetype = leasetype;
    }

    public Shower getShower() {
        return shower;
    }

    public void setShower(Shower shower) {
        this.shower = shower;
    }

    public Aircondition getAircondition() {
        return aircondition;
    }

    public void setAircondition(Aircondition aircondition) {
        this.aircondition = aircondition;
    }

    public Tv getTv() {
        return tv;
    }

    public void setTv(Tv tv) {
        this.tv = tv;
    }

    public Housetyperoomcnt getHousetyperoomcnt() {
        return housetyperoomcnt;
    }

    public void setHousetyperoomcnt(Housetyperoomcnt housetyperoomcnt) {
        this.housetyperoomcnt = housetyperoomcnt;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public Guestnum getGuestnum() {
        return guestnum;
    }

    public void setGuestnum(Guestnum guestnum) {
        this.guestnum = guestnum;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public class Extend{
        String type;
        Map<String,String> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getValue() {
            return value;
        }

        public void setValue(Map<String, String> value) {
            this.value = value;
        }
    }

    public class Bookables{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Days{
        String type;
        Map<String,Object> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getValue() {
            return value;
        }

        public void setValue(Map<String, Object> value) {
            this.value = value;
        }
    }

    public class Lodgeunittags{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Latlng{
        String type;
        Map<String,String> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getValue() {
            return value;
        }

        public void setValue(Map<String, String> value) {
            this.value = value;
        }
    }

    public class Price{
        String type;
        Map<String,Object> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getValue() {
            return value;
        }

        public void setValue(Map<String, Object> value) {
            this.value = value;
        }
    }

    public class Leasetype{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Shower{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Aircondition{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Tv{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Housetyperoomcnt{
        String type;
        List<Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Integer> getValue() {
            return value;
        }

        public void setValue(List<Integer> value) {
            this.value = value;
        }
    }

    public class Sort{
        String type;
        Map<String,Object> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getValue() {
            return value;
        }

        public void setValue(Map<String, Object> value) {
            this.value = value;
        }
    }

    public class Guestnum{
        String type;
        Map<String,Object> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getValue() {
            return value;
        }

        public void setValue(Map<String, Object> value) {
            this.value = value;
        }
    }

    public class Page{
        String type;
        Map<String,Integer> value;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Integer> getValue() {
            return value;
        }

        public void setValue(Map<String, Integer> value) {
            this.value = value;
        }
    }

    public class OccurTime {

        private String type;
        private String value;
        public void setType(String type) {
            this.type = type;
        }
        public String getType() {
            return type;
        }

        public void setValue(String value) {
            this.value = value;
        }
        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            if(value==null){
                value="";
            }
            return value.replaceAll("[\b\r\n\t]*", "");
        }
    }
}









