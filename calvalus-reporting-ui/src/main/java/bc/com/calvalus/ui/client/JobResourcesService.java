package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import java.util.List;

/**
 * @author muhammad.bc.
 */
@RemoteServiceRelativePath("JobResourcesService")
public interface JobResourcesService extends RemoteService {

    List<String> getAllUserName();

    List<UserInfo> getAllUserSummary(String userName, String startDate, String endDate);

    List<UserInfo> getAllUserTodaySummary();
}
