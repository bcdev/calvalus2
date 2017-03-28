package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * @author muhammad.bc.
 */
@RemoteServiceRelativePath("JobResourcesService")
public interface JobResourcesService extends RemoteService {

    UserInfoInDetails getAllUserUsageForToday(ColumnType columnType);

    UserInfoInDetails getAllUserUsageForThisWeek(ColumnType columnType);

    UserInfoInDetails getAllUserUsageForThisMonth(ColumnType columnType);

    UserInfoInDetails getAllUserUsageForLastWeek(ColumnType columnType);

    UserInfoInDetails getAllUserUsageForLastMonth(ColumnType columnType);

    UserInfoInDetails getAllUserUsageForYesterday(ColumnType columnType);

    UserInfoInDetails getAllUserUsageBetween(String startDate, String endDate, ColumnType columnType);
}
