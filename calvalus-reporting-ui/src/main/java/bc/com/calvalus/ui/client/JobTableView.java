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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
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
        userName.setSortable(true);
        sortHandler.setComparator(userName, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            return o11.compareTo(o12);
        });
        dataGrid.addColumn(userName, "User Name");
        dataGrid.setColumnWidth(userName, 10, Style.Unit.EM);

        // ### jobsProcessed
        Column<T, String> jobsProcessed = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getJobsProcessed();
            }
        };

        jobsProcessed.setSortable(true);
        sortHandler.setComparator(jobsProcessed, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            Integer integerO = new Integer(o11.getJobsProcessed());
            Integer integerM = new Integer(o12.getJobsProcessed());
            return integerO.compareTo(integerM);
        });
        dataGrid.addColumn(jobsProcessed, "Jobs");
        dataGrid.setColumnWidth(jobsProcessed, 20, Style.Unit.PCT);

        // #### products
        Column<T, String> products = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalMaps();
            }
        };
        products.setSortable(true);
        sortHandler.setComparator(products, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            String longVal1 = o11.getTotalMaps().replace(",", "");
            String longVal2 = o12.getTotalMaps().replace(",", "");
            return longVal1.compareTo(longVal2);
        });
        dataGrid.addColumn(products, "Products");
        dataGrid.setColumnWidth(products, 20, Style.Unit.PCT);


        // #### cpuHours
        Column<T, String> cpuHours = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalCpuTimeSpent();
            }
        };
        cpuHours.setSortable(true);
        sortHandler.setComparator(cpuHours, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            String longVal1 = o11.getTotalCpuTimeSpent().replace(",", "");
            String longVal2 = o12.getTotalCpuTimeSpent().replace(",", "");
            return longVal1.compareTo(longVal2);
        });
        dataGrid.addColumn(cpuHours, "Cpu Hours");
        dataGrid.setColumnWidth(cpuHours, 20, Style.Unit.PCT);


        // ### totalMemoryUsedMbs
        Column<T, String> totalMemoryUsedMbs = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalMemoryUsedMbs();
            }
        };
        totalMemoryUsedMbs.setSortable(true);
        sortHandler.setComparator(totalMemoryUsedMbs, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            String longVal1 = o11.getTotalMemoryUsedMbs().replace(",", "");
            String longVal2 = o12.getTotalMemoryUsedMbs().replace(",", "");
            return longVal1.compareTo(longVal2);
        });
        dataGrid.addColumn(totalMemoryUsedMbs, "RAM GB hours");
        dataGrid.setColumnWidth(totalMemoryUsedMbs, 20, Style.Unit.PCT);


        // ### totalFileReadingMb
        Column<T, String> totalFileReadingMb = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalFileReadingMb();
            }
        };
        totalFileReadingMb.setSortable(true);
        sortHandler.setComparator(totalFileReadingMb, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            String longVal1 = o11.getTotalFileReadingMb().replace(",", "");
            String longVal2 = o12.getTotalFileReadingMb().replace(",", "");
            return longVal1.compareTo(longVal2);
        });
        dataGrid.addColumn(totalFileReadingMb, "TB Read");
        dataGrid.setColumnWidth(totalFileReadingMb, 20, Style.Unit.PCT);

        // ### totalFileWritingMb
        Column<T, String> totalFileWritingMb = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalFileWritingMb();
            }
        };
        totalFileWritingMb.setSortable(true);
        sortHandler.setComparator(totalFileWritingMb, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            String longVal1 = o11.getTotalFileWritingMb().replace(",", "");
            String longVal2 = o12.getTotalFileWritingMb().replace(",", "");
            return longVal1.compareTo(longVal2);
        });
        dataGrid.addColumn(totalFileWritingMb, "TB Write");
        dataGrid.setColumnWidth(totalFileWritingMb, 20, Style.Unit.PCT);
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


