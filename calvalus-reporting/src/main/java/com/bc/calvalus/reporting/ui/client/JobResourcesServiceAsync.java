package com.bc.calvalus.reporting.ui.client;

import com.bc.calvalus.reporting.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.AsyncCallback;

public interface JobResourcesServiceAsync {

    void getAllUserUsageForToday(ColumnType columnType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserUsageForThisWeek(ColumnType columnType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserUsageForThisMonth(ColumnType columnType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserUsageForLastWeek(ColumnType columnType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserUsageForLastMonth(ColumnType columnType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserUsageForYesterday(ColumnType columnType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserUsageBetween(String startDate, String endDate, ColumnType columnType, AsyncCallback<UserInfoInDetails> async);
}
