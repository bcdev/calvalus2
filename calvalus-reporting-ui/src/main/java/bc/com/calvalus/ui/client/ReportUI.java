package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;

public class ReportUI implements EntryPoint {
    private static final JobResourcesServiceAsync resourcesServiceAsync = GWT.create(JobResourcesService.class);
    private static final JobTableView<UserInfo> infoReportTable = new JobTableView<>();
    private static final SearchPanel searchPanel = new SearchPanel();
    private static final Label label = new Label();


    @Override
    public void onModuleLoad() {
        initReportSelection();
        RootPanel.get("displayDateInterval").add(label);
        RootPanel.get("searchBarUI").add(searchPanel);
        RootPanel.get("tableDisplayId").add(infoReportTable);
    }

    static void setColumnType(ColumnType columnType) {
        switch (columnType) {
            case USER:
                infoReportTable.initUserTable();
                break;
            case DATE:
                infoReportTable.initDateTable();
                break;
            case QUEUE:
                infoReportTable.initQueueTable();
                break;
        }
        searchRecordForThisWeek(columnType);
    }

    static void searchRecordForToday(ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageForToday(columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in loading the table" + throwable.getMessage()));
//                Window.open("failure-page.html", "_self", "");
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);

            }
        });
    }

    static void searchRecordForYesterday(ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageForYesterday(columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in load yesterday data to the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);
            }
        });
    }

    static void searchRecordForThisWeek(ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageForThisWeek(columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in loading this week data the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);
            }
        });
    }

    static void searchRecordForThisMonth(ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageForThisMonth(columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in loading this month data to the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfos) {
                infoReportTable.setDataList(userInfos.getUserInfos());
                displayDateInterval(userInfos);
            }
        });
    }

    static void searchRecordForLastWeek(ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageForLastWeek(columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in loading last week data to the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfoInDetails) {
                infoReportTable.setDataList(userInfoInDetails.getUserInfos());
                displayDateInterval(userInfoInDetails);
            }
        });
    }

    static void searchRecordForLastMonth(ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageForLastMonth(columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable throwable) {
                RootPanel.get().add(new HTML("Error in loading the table" + throwable.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails userInfoInDetails) {
                infoReportTable.setDataList(userInfoInDetails.getUserInfos());
                displayDateInterval(userInfoInDetails);
            }
        });
    }

    static void searchRecordBetween(String start, String end, ColumnType columnType) {
        resourcesServiceAsync.getAllUserUsageBetween(start, end, columnType, new AsyncCallback<UserInfoInDetails>() {
            @Override
            public void onFailure(Throwable caught) {
                RootPanel.get().add(new HTML("Error in loading data from " + start + " to " + end + " the table" + caught.getMessage()));
            }

            @Override
            public void onSuccess(UserInfoInDetails result) {
                infoReportTable.setDataList(result.getUserInfos());
                displayDateInterval(result);
            }
        });
    }

    private static void displayDateInterval(UserInfoInDetails userInfos) {
        if (userInfos.getStartDate() != null && userInfos.getEndDate() != null) {
            label.setText("Report : " + userInfos.getStartDate() + "  to " + userInfos.getEndDate());
            searchPanel.updateDatePicker(userInfos.getStartDate(), userInfos.getEndDate());
        }
    }

    private void initReportSelection() {
        searchRecordForThisWeek(ColumnType.DATE);
    }
}
