package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.googlecode.gwt.charts.client.ChartLoader;
import com.googlecode.gwt.charts.client.ChartPackage;
import com.googlecode.gwt.charts.client.ColumnType;
import com.googlecode.gwt.charts.client.DataTable;
import com.googlecode.gwt.charts.client.Selection;
import com.googlecode.gwt.charts.client.corechart.PieChart;
import com.googlecode.gwt.charts.client.corechart.PieChartOptions;
import com.googlecode.gwt.charts.client.event.ReadyEvent;
import com.googlecode.gwt.charts.client.event.ReadyHandler;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class UsageChart extends AbsolutePanel {
    private PieChart chart;

    public UsageChart() {
        this.chart = chart;
    }

    public void initialize(List<UserInfo> result, String startDate, String endDate) {
        ChartLoader chartLoader = new ChartLoader(ChartPackage.CORECHART);
        chartLoader.loadApi(new Runnable() {

            @Override
            public void run() {
                // Create and attach the chart
                clear();
                chart = new PieChart();
                add(chart);
                draw(result, startDate, endDate);
                chart.redraw();
            }
        });
    }

    private void draw(List<UserInfo> result, String startDate, String endDate) {
        // Prepare the data
        DataTable dataTable = DataTable.create();
        dataTable.addColumn(ColumnType.STRING, "User Name");
        dataTable.addColumn(ColumnType.NUMBER, "Jobs Processed");
        dataTable.addRows(result.size());
        for (int i = 0; i < result.size(); i++) {
            dataTable.setValue(i, 0, result.get(i).getUser());
        }
        for (int i = 0; i < result.size(); i++) {
            dataTable.setValue(i, 1, result.get(i).getJobsProcessed());
        }
        // Set options
        PieChartOptions options = PieChartOptions.create();
        options.setBackgroundColor("#f0f0f0");

        // options.setColors(colors);
        options.setFontName("Tahoma");
        options.setIs3D(false);
        options.setPieResidueSliceColor("#000000");
        options.setPieResidueSliceLabel("Others");
        options.setSliceVisibilityThreshold(0.1);
        options.setTitle("Usage summary from");

        // Draw the chart
        chart.draw(dataTable, options);
        chart.addReadyHandler(new ReadyHandler() {

            @Override
            public void onReady(ReadyEvent event) {
                chart.setSelection(Selection.create(1, null));
            }
        });
    }
}
