package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * @author muhammad.bc.
 */
@RemoteServiceRelativePath("JobResourcesService")
public interface JobResourcesService extends RemoteService {

    UserInfoInDetails getAllUserTodaySummary();

    UserInfoInDetails getAllUserWeekAgoSummary();

    UserInfoInDetails getAllUserMonthAgoSummary();

    UserInfoInDetails getAllUserYesterdaySummary();

    UserInfoInDetails getAllUserSummaryBetween(String startDate, String endDate);
}
