package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Style;
import com.google.gwt.safehtml.shared.SafeHtml;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.text.shared.AbstractSafeHtmlRenderer;
import com.google.gwt.text.shared.SafeHtmlRenderer;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.ColumnSortEvent;
import com.google.gwt.user.cellview.client.DataGrid;
import com.google.gwt.user.cellview.client.SimplePager;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.DockPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.view.client.ListDataProvider;
import java.util.ArrayList;
import java.util.List;

/**
 * @author muhammad.bc.
 */

public class JobTableView<T> extends Composite {
    public static final String WIDTH_100_PERCENT = "100%";
    public static final boolean SHOW_FAST_FORWARD_BUTTON = false;
    public static final int FAST_FORWARD_ROWS = 0;
    public static final boolean SHOW_LAST_PAGE_BUTTON = true;
    private DataGrid<T> dataGrid;
    private SimplePager pager;
    private String height;
    private ListDataProvider<T> dataProvider;
    private List<T> dataList;
    private DockPanel dock = new DockPanel();

    public JobTableView() {
        initWidget(dock);
        dataGrid = new DataGrid<T>();
        dataGrid.setWidth(WIDTH_100_PERCENT);
        dataGrid.setHeight("700px");

        SimplePager.Resources pagerResources = GWT.create(SimplePager.Resources.class);
        pager = new SimplePager(SimplePager.TextLocation.CENTER, pagerResources, SHOW_FAST_FORWARD_BUTTON, FAST_FORWARD_ROWS, SHOW_LAST_PAGE_BUTTON);
        pager.setDisplay(dataGrid);
        dataProvider = new ListDataProvider<T>();
        dataProvider.setList(new ArrayList<T>());
        dataGrid.setEmptyTableWidget(new HTML("No Data to Display"));
        ColumnSortEvent.ListHandler<T> sortHandler = new ColumnSortEvent.ListHandler<T>(dataProvider.getList());

        initTableColumns(dataGrid, sortHandler);

        dataGrid.addColumnSortHandler(sortHandler);
        dataProvider.addDataDisplay(dataGrid);
        pager.setVisible(true);
        dataGrid.setVisible(true);

        dock.add(dataGrid, DockPanel.CENTER);
        dock.add(pager, DockPanel.SOUTH);
        dock.setWidth(WIDTH_100_PERCENT);
        dock.setCellWidth(dataGrid, WIDTH_100_PERCENT);
        dock.setCellWidth(pager, WIDTH_100_PERCENT);
        dock.setCellHorizontalAlignment(pager, HasHorizontalAlignment.ALIGN_CENTER);
    }

    public void setHeight(String height) {
        this.height = height;
        dataGrid.setHeight(height);
    }

    public void setDataList(List<T> dataList) {
        List<T> list = dataProvider.getList();
        list.clear();
        if (dataList == null) {
            dataGrid.setEmptyTableWidget(new HTML("No Data to Display"));
        } else {
            list.addAll(dataList);
        }
        dataProvider.refresh();
        dataGrid.redraw();

    }

    public void initTableColumns(DataGrid<T> dataGrid, ColumnSortEvent.ListHandler<T> sortHandler) {
        SafeHtmlRenderer<String> anchorRenderer = new AbstractSafeHtmlRenderer<String>() {
            public SafeHtml render(String object) {
                SafeHtmlBuilder sb = new SafeHtmlBuilder();
                sb.appendHtmlConstant("(<a href=\"javascript:;\">").appendEscaped(object)
                        .appendHtmlConstant("</a>)");
                return sb.toSafeHtml();
            }
        };

        Column userName = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getUser();
            }
        };
        dataGrid.addColumn(userName, "User Name");
        dataGrid.setColumnWidth(userName, 10, Style.Unit.EM);

        // ### jobsProcessed
        Column<T, String> jobsProcessed = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getJobsProcessed();
            }
        };
        dataGrid.addColumn(jobsProcessed, "Jobs Processed");
        dataGrid.setColumnWidth(jobsProcessed, 20, Style.Unit.PCT);

        // ### totalFileReadingMb
        Column<T, String> totalFileReadingMb = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalFileReadingMb();
            }
        };
        dataGrid.addColumn(totalFileReadingMb, "Total File Read GBs");
        dataGrid.setColumnWidth(totalFileReadingMb, 20, Style.Unit.PCT);

        // ### totalFileWritingMb
        Column<T, String> totalFileWritingMb = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalFileWritingMb();
            }
        };
        dataGrid.addColumn(totalFileWritingMb, "Total File Write GBs");
        dataGrid.setColumnWidth(totalFileWritingMb, 20, Style.Unit.PCT);

        // ### totalMemoryUsedMbs
        Column<T, String> totalMemoryUsedMbs = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalMemoryUsedMbs();
            }
        };
        dataGrid.addColumn(totalMemoryUsedMbs, "Total Memory Used TBs");
        dataGrid.setColumnWidth(totalMemoryUsedMbs, 20, Style.Unit.PCT);

        // #### totalCpuTimeSpent
        Column<T, String> totalCpuTimeSpent = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalCpuTimeSpent();
            }
        };
        dataGrid.addColumn(totalCpuTimeSpent, "Total Cpu Time Spent");
        dataGrid.setColumnWidth(totalCpuTimeSpent, 20, Style.Unit.PCT);

        // #### totalVcoresUsed
        Column<T, String> totalMaps = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalMaps();
            }
        };
        dataGrid.addColumn(totalMaps, "Total Maps");
        dataGrid.setColumnWidth(totalMaps, 20, Style.Unit.PCT);
    }

    public DialogBox getShowDialog(UserInfo userInfo) {
        DialogBox dialogBox = new DialogBox();
        dialogBox.setAnimationEnabled(true);
        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setHorizontalAlignment(VerticalPanel.ALIGN_LEFT);
        dialogBox.setWidget(verticalPanel);
        dialogBox.center();
        dialogBox.hide();
        return dialogBox;
    }
}


