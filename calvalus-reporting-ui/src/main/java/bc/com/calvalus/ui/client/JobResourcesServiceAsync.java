package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.AsyncCallback;

public interface JobResourcesServiceAsync {

    void getAllUserTodaySummary(TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserThisWeekSummary(TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserThisMonthSummary(TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserLastWeekSummary(TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserLastMonthSummary(TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserYesterdaySummary(TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void getAllUserSummaryBetween(String startDate, String endDate, TableType tableType, AsyncCallback<UserInfoInDetails> async);

    void compareDate(String start, String end, AsyncCallback<Integer> async);
}
