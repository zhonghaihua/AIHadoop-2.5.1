package org.apache.hadoop.security.blackwhitelist;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by iwardzhong on 2016/11/24.
 */
public class BlackWhiteList {

    private String ip = "default";
    private List<String> userList = new ArrayList<String>();
    private String description = "default";

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public List<String> getUserList() {
        return userList;
    }

    public void setUserList(List<String> userList) {
        this.userList = userList;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
