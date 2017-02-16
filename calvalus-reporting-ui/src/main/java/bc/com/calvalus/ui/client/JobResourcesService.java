package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * @author muhammad.bc.
 */
@RemoteServiceRelativePath("JobResourcesService")
public interface JobResourcesService extends RemoteService {

    UserInfoInDetails getAllUserTodaySummary(TableType tableType);

    UserInfoInDetails getAllUserThisWeekSummary(TableType tableType);

    UserInfoInDetails getAllUserThisMonthSummary(TableType tableType);

    UserInfoInDetails getAllUserLastWeekSummary(TableType tableType);

    UserInfoInDetails getAllUserLastMonthSummary(TableType tableType);

    UserInfoInDetails getAllUserYesterdaySummary(TableType tableType);

    UserInfoInDetails getAllUserSummaryBetween(String startDate, String endDate,TableType tableType);

    int compareDate(String start,String end);
}
