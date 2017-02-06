package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.user.client.rpc.AsyncCallback;
import java.util.List;

public interface JobResourcesServiceAsync {

    void getAllUserSummary(String userName, String startDate, String endDate, AsyncCallback<List<UserInfo>> async);

    void getAllUserName(AsyncCallback<List<String>> async);

    void getAllUserTodaySummary(AsyncCallback<List<UserInfo>> async);
}
