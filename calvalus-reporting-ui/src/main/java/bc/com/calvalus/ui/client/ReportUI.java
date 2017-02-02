package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.RootPanel;
import java.util.List;


/**
 */
public class ReportUI implements EntryPoint {
    private static final JobResourcesServiceAsync resourcesServiceAsync = GWT.create(JobResourcesService.class);
    private static JobTableView<UserInfo> infoReportTable = new JobTableView<>();
    private SearchPanel searchPanel;

    public ReportUI() {
    }

    @Override
    public void onModuleLoad() {

        initDisplayTableandChart();
        resourcesServiceAsync.getAllUserName(new AsyncCallback<List<String>>() {
            @Override
            public void onFailure(Throwable caught) {
                RootPanel.get().add(new HTML("Error in loading search bar" + caught.getMessage()));
            }

            @Override
            public void onSuccess(List<String> result) {
                searchPanel = new SearchPanel(result);
                RootPanel.get("searchBarUI").add(searchPanel);
            }
        });

        RootPanel.get("tableDisplayId").add(infoReportTable);
    }

    private void initDisplayTableandChart() {
        searchRecord(null, null, null);
    }

    static void searchRecord(String start, String end, String userName) {
        resourcesServiceAsync.getAllUserSummary(userName, start, end, new AsyncCallback<List<UserInfo>>() {
            @Override
            public void onFailure(Throwable caught) {
                RootPanel.get().add(new HTML("Error in load Pie or the table" + caught.getMessage()));
            }

            @Override
            public void onSuccess(List<UserInfo> result) {
                infoReportTable.setDataList(result);
            }
        });

    }


}
