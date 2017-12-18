package com.bc.calvalus.reporting.ui.shared;

import com.google.gwt.user.client.rpc.IsSerializable;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class UserInfoInDetails implements IsSerializable {
    List<UserInfo> userInfos;
    String startDate;
    String endDate;

    public UserInfoInDetails(List<UserInfo> userInfos, String startDate, String endDate) {
        this.userInfos = userInfos;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public UserInfoInDetails() {
    }

    public List<UserInfo> getUserInfos() {
        return userInfos;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getEndDate() {
        return endDate;
    }
}
