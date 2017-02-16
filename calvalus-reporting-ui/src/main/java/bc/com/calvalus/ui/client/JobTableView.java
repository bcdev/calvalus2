package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.builder.shared.TableRowBuilder;
import com.google.gwt.dom.client.Style;
import com.google.gwt.user.cellview.client.AbstractHeaderOrFooterBuilder;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.ColumnSortEvent;
import com.google.gwt.user.cellview.client.DataGrid;
import com.google.gwt.user.cellview.client.SimplePager;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DockPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.view.client.ListDataProvider;
import java.util.ArrayList;
import java.util.List;

/**
 * @author muhammad.bc.
 */

public class JobTableView<T> extends Composite {
    private static final String WIDTH_100_PERCENT = "100%";
    private static final boolean SHOW_FAST_FORWARD_BUTTON = false;
    private static final int FAST_FORWARD_ROWS = 0;
    private static final boolean SHOW_LAST_PAGE_BUTTON = true;
    private DataGrid<T> dataGrid;
    private ListDataProvider<T> dataProvider;
    private List<T> dataList;
    private DockPanel dock = new DockPanel();
    private final ColumnSortEvent.ListHandler<T> sortHandler;

    JobTableView() {
        initWidget(dock);
        dataGrid = new DataGrid<T>();
        dataGrid.setWidth(WIDTH_100_PERCENT);
        dataGrid.setHeight("700px");

        SimplePager.Resources pagerResources = GWT.create(SimplePager.Resources.class);
        SimplePager pager = new SimplePager(SimplePager.TextLocation.CENTER, pagerResources, SHOW_FAST_FORWARD_BUTTON, FAST_FORWARD_ROWS, SHOW_LAST_PAGE_BUTTON);
        pager.setDisplay(dataGrid);
        dataProvider = new ListDataProvider<T>();
        dataProvider.setList(new ArrayList<T>());
        dataGrid.setEmptyTableWidget(new HTML("No Data to Display"));
        sortHandler = new ColumnSortEvent.ListHandler<T>(dataProvider.getList());

        initTableColumns(dataGrid, sortHandler);

        dataGrid.addColumnSortHandler(sortHandler);
        dataProvider.addDataDisplay(dataGrid);
        pager.setVisible(true);
        dataGrid.setVisible(true);
        dataGrid.setFooterBuilder(new SumColumnValueFooterBuilder());

        dock.add(dataGrid, DockPanel.CENTER);
        dock.add(pager, DockPanel.SOUTH);
        dock.setWidth(WIDTH_100_PERCENT);
        dock.setCellWidth(dataGrid, WIDTH_100_PERCENT);
        dock.setCellWidth(pager, WIDTH_100_PERCENT);
        dock.setCellHorizontalAlignment(pager, HasHorizontalAlignment.ALIGN_CENTER);
    }

    void setDataList(List<T> dataList) {
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

    void initDateTable() {
        initNewColumn();
        Column dateTime = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getJobsInDate();
            }
        };
        dateTime.setSortable(true);
        sortHandler.setComparator(dateTime, (o1, o2) -> {
            String firstDate = ((UserInfo) o1).getJobsInDate();
            String secondDate = ((UserInfo) o1).getJobsInDate();
            return ReportUI.compareDate(firstDate, secondDate);

        });
        dataGrid.insertColumn(0, dateTime, "Date");
        dataGrid.setColumnWidth(dateTime, 10, Style.Unit.EM);
    }

    void initUserTable() {
        initNewColumn();
        Column user = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getUser();
            }
        };
        user.setSortable(true);
        sortHandler.setComparator(user, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return o11.compareTo(o12);
        });
        dataGrid.insertColumn(0, user, "User");
        dataGrid.setColumnWidth(user, 10, Style.Unit.EM);
    }

    void initQueueTable() {
        initNewColumn();
        Column queue = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getJobsInQueue();
            }
        };
        queue.setSortable(true);
        sortHandler.setComparator(queue, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return o11.compareTo(o12);
        });
        dataGrid.insertColumn(0, queue, "Queue");
        dataGrid.setColumnWidth(queue, 10, Style.Unit.EM);
    }

    private void initNewColumn() {
        dataGrid.removeColumn(0);
        List<T> list = dataProvider.getList();
        list.clear();
        dataProvider.refresh();
        dataGrid.redraw();
    }


    private void initTableColumns(DataGrid<T> dataGrid, ColumnSortEvent.ListHandler<T> sortHandler) {
        Column dateTime = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getJobsInDate();
            }
        };
        dateTime.setSortable(true);
        sortHandler.setComparator(dateTime, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            return o11.compareTo(o12);
        });
        dataGrid.addColumn(dateTime, "Date");
        dataGrid.setColumnWidth(dateTime, 10, Style.Unit.EM);

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
                return ((UserInfo) object).getTotalMapReduce();
            }
        };
        products.setSortable(true);
        sortHandler.setComparator(products, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            String longVal1 = o11.getTotalMapReduce().replace(",", "");
            String longVal2 = o12.getTotalMapReduce().replace(",", "");
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

    private class SumColumnValueFooterBuilder extends AbstractHeaderOrFooterBuilder<T> {
        SumColumnValueFooterBuilder() {
            super(dataGrid, true);
        }

        @Override
        protected boolean buildHeaderOrFooterImpl() {
            List<UserInfo> visibleData = (List<UserInfo>) dataGrid.getVisibleItems();
            if (visibleData.size() > 0) {
                int totalTBWrite = 0;
                int totalTBRead = 0;
                int totalProduct = 0;

                for (UserInfo visibleDatum : visibleData) {
                    totalTBRead += Long.parseLong(visibleDatum.getTotalFileReadingMb());
                    totalProduct += Long.parseLong(visibleDatum.getJobsProcessed());
                    totalTBWrite += Long.parseLong(visibleDatum.getTotalFileWritingMb());
                }
            }
            TableRowBuilder tableRowBuilder = startRow();
            TableRowBuilder tableRow = tableRowBuilder.align(HasHorizontalAlignment.ALIGN_CENTER.getTextAlignString());
            return false;
        }
    }
}


