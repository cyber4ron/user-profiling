package db.domain;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * Created by huangfangsheng on 16/6/12.
 */

public class FollowInfo {
    public int favorite_id;
    public long user_id;
    public int notification_status;
    public String favorite_type;
    public int source_type;
    public String favorite_condition;
    public String condition_detail;
    public int bit_status;
    public String ctime;
    public String mtime;
    public String city_id;

    public int getFavorite_id() {
        return favorite_id;
    }

    public void setFavorite_id(int favorite_id) {
        this.favorite_id = favorite_id;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public int getNotification_status() {
        return notification_status;
    }

    public void setNotification_status(int notification_status) {
        this.notification_status = notification_status;
    }

    public String getFavorite_type() {
        return favorite_type;
    }

    public void setFavorite_type(String favorite_type) {
        this.favorite_type = favorite_type;
    }

    public int getSource_type() {
        return source_type;
    }

    public void setSource_type(int source_type) {
        this.source_type = source_type;
    }

    public String getFavorite_condition() {
        return favorite_condition;
    }

    public void setFavorite_condition(String favorite_condition) {
        this.favorite_condition = favorite_condition;
    }

    public String getCondition_detail() {
        return condition_detail;
    }

    public void setCondition_detail(String condition_detail) {
        this.condition_detail = condition_detail;
    }

    public int getBit_status() {
        return bit_status;
    }

    public void setBit_status(int bit_status) {
        this.bit_status = bit_status;
    }

    public String getCtime() {
        return ctime;
    }

    public void setCtime(String ctime) throws ParseException {
        this.ctime = dfES.format(df.parse(ctime));
    }

    public String getMtime() {
        return mtime;
    }

    public void setMtime(String mtime) throws ParseException {
        this.mtime = dfES.format(df.parse(mtime));
    }

    public FollowInfo() {

    }

    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat dfES = new SimpleDateFormat("yyyyMMdd'T'HHmmssZ");

    public FollowInfo(int favorite_id, long user_id, int notification_status,
                      String favorite_type, int source_type, String favorite_condition,
                      String condition_detail, int bit_status, String ctime, String mtime, String city_id) throws ParseException {
        this.favorite_id = favorite_id;
        this.user_id = user_id;
        this.notification_status = notification_status;
        this.favorite_type = favorite_type;
        this.source_type = source_type;
        this.favorite_condition = favorite_condition;
        this.condition_detail = condition_detail;
        this.bit_status = bit_status;
        this.ctime = dfES.format(df.parse(ctime));
        this.mtime = dfES.format(df.parse(mtime));
        this.city_id = city_id;
    }

    public HashMap<String, Object> toHashMap() {
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("fid", this.favorite_id);
        ret.put("ucid", this.user_id);
        ret.put("type", this.favorite_type);

//        String follow_source = null;
//        switch (this.source_type) {
//            case 101:
//                follow_source = "链家网";
//                break;
//            case 102:
//                follow_source = "m站";
//                break;
//            case 103:
//                follow_source = "掌上链家";
//                break;
//            default:
//                follow_source = "链家网";
//        }

        ret.put("src", this.source_type);
        ret.put("status", this.bit_status);
        ret.put("ctime", this.ctime);
        ret.put("mtime", this.mtime);
        ret.put("target", this.favorite_condition);
        ret.put("city_id", this.city_id);
        ret.put("write_ts", System.currentTimeMillis());

        return ret;
    }

}
