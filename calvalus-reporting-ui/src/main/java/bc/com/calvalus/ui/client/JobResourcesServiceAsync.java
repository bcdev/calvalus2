package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.AsyncCallback;

public interface JobResourcesServiceAsync {

    void getAllUserTodaySummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserThisWeekSummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserThisMonthSummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserYesterdaySummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserSummaryBetween(String startDate, String endDate, AsyncCallback<UserInfoInDetails> async);

    void getAllUserLastMonthSummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserLastWeekSummary(AsyncCallback<UserInfoInDetails> async);
}
