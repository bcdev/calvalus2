package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;


/**
 */
public class ReportUI implements EntryPoint {
    private static final JobResourcesServiceAsync resourcesServiceAsync = GWT.create(JobResourcesService.class);
    private static final JobTableView<UserInfo> infoReportTable = new JobTableView<>();
    private static final SearchPanel searchPanel = new SearchPanel();
    private static final Label label = new Label();

    @Override
    public void onModuleLoad() {
        initDisplayTableandChart();
        RootPanel.get("displayDateInterval").add(label);
        RootPanel.get("searchBarUI").add(searchPanel);
        RootPanel.get("tableDisplayId").add(infoReportTable);
    }

    static void searchRecordYesterday() {
        resourcesServiceAsync.getAllUserYesterdaySummary(new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in load Pie or the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);
            }
        });
    }

    static void searchRecordToday() {
        resourcesServiceAsync.getAllUserTodaySummary(new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in loading the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);

            }
        });
    }

    private static void displayDateInterval(UserInfoInDetails userInfos) {
        if (userInfos.getStartDate() != null && userInfos.getEndDate() != null) {
            label.setText("Search result from : " + userInfos.getStartDate() + "  to " + userInfos.getEndDate());
        }
    }

    static void searchRecordWeekAgo() {
        resourcesServiceAsync.getAllUserWeekAgoSummary(new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in load Pie or the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);
            }
        });
    }

    static void searchRecordMonthAgo() {
        resourcesServiceAsync.getAllUserMonthAgoSummary(new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in load Pie or the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);
            }
        });
    }


    static void searchRecord(String start, String end) {
        resourcesServiceAsync.getAllUserSummaryBetween(start, end, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable caught) {
                RootPanel.get().add(new HTML("Error in load Pie or the table" + caught.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails result) {
                infoReportTable.setDataList(result.getUserInfos());
                displayDateInterval(result);
            }
        });
    }


    private void initDisplayTableandChart() {
        searchRecord(null, null);
    }
}
