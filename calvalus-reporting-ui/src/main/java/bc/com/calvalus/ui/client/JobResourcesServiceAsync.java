package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.AsyncCallback;

public interface JobResourcesServiceAsync {

    void getAllUserTodaySummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserWeekAgoSummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserMonthAgoSummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserYesterdaySummary(AsyncCallback<UserInfoInDetails> async);

    void getAllUserSummaryBetween(String startDate, String endDate, AsyncCallback<UserInfoInDetails> async);
}
