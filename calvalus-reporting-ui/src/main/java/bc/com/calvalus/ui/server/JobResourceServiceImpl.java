package bc.com.calvalus.ui.server;

import bc.com.calvalus.ui.client.ColumnType;
import bc.com.calvalus.ui.client.JobResourcesService;
import bc.com.calvalus.ui.shared.UserInfo;
import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author muhammad.bc.
 */

public class JobResourceServiceImpl extends RemoteServiceServlet implements JobResourcesService {
    static final String STATUS_FAILED = "\"Status\": \"Failed\"";
    String calvalusReportingWebServicesUrl;
    static final int HTTP_SUCCESSFUL_CODE_START = 200;
    static final int HTTP_SUCCESSFUL_CODE_END = 300;

    static final int TO_GB = 1024;
    static final int FIRST_DAY = 1;

    public JobResourceServiceImpl() throws IOException {
        try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("calvalus-reporting.properties")) {
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            calvalusReportingWebServicesUrl = (String) properties.get("reporting.webservice");
    }
    }

    @Override
    public UserInfoInDetails getAllUserUsageForToday(ColumnType columnType) {
        LocalDate now = LocalDate.now();
        return getAllUserUsageBetween(now.toString(), now.toString(), columnType);
    }

    @Override
    public UserInfoInDetails getAllUserUsageForThisWeek(ColumnType columnType) {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(endDate.getDayOfWeek().getValue() - 1);
        return getAllUserUsageBetween(startDate.toString(), endDate.toString(), columnType);
    }

    @Override
    public UserInfoInDetails getAllUserUsageForLastWeek(ColumnType columnType) {
        LocalDate now = LocalDate.now();
        DayOfWeek dayOfWeek = now.getDayOfWeek();

        LocalDate endDate = now.minusDays(dayOfWeek.getValue());
        LocalDate startDate = endDate.minusDays(6);
        return getAllUserUsageBetween(startDate.toString(), endDate.toString(), columnType);
    }

    @Override
    public UserInfoInDetails getAllUserUsageForThisMonth(ColumnType columnType) {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.withDayOfMonth(1);
        return getAllUserUsageBetween(startDate.toString(), endDate.toString(), columnType);
    }

    @Override
    public UserInfoInDetails getAllUserUsageForLastMonth(ColumnType columnType) {
        LocalDate now = LocalDate.now();
        now = now.minusMonths(1);
        LocalDate startDate = now.withDayOfMonth(1);
        GregorianCalendar calendar = new GregorianCalendar();
        int dayOfMonth = now.getMonth().minLength();
        if (calendar.isLeapYear(now.getYear())) {
            dayOfMonth = now.getMonth().maxLength();
        }
        LocalDate endDate = now.withDayOfMonth(dayOfMonth);

        return getAllUserUsageBetween(startDate.toString(), endDate.toString(), columnType);
    }


    @Override
    public UserInfoInDetails getAllUserUsageForYesterday(ColumnType columnType) {
        LocalDate startDate = LocalDate.now().minusDays(FIRST_DAY);
        return getAllUserUsageBetween(startDate.toString(), startDate.toString(), columnType);
    }

    @Override
    public UserInfoInDetails getAllUserUsageBetween(String startDate, String endDate, ColumnType columnType) {
        List<UserInfo> allUsageSummaryBetween = null;
        switch (columnType) {
            case USER:
                allUsageSummaryBetween = getAllUserUsageBetween(startDate, endDate);
                break;
            case DATE:
                allUsageSummaryBetween = getAllDateUsageBetween(startDate, endDate);
                break;
            case QUEUE:
                allUsageSummaryBetween = getAllQueueUsageBetween(startDate, endDate);
                break;
        }
        return new UserInfoInDetails(allUsageSummaryBetween, startDate, endDate);
    }

    private List<UserInfo> getAllDateUsageBetween(String startDate, String endDate) {
        String jsonUser = clientRequest(String.format(calvalusReportingWebServicesUrl.concat("/range-date/%s/%s"), startDate, endDate), MediaType.TEXT_PLAIN);
        return gsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getAllUserUsageBetween(String startDate, String endDate) {
        String jsonUser = clientRequest(String.format(calvalusReportingWebServicesUrl.concat("/range-user/%s/%s"), startDate, endDate), MediaType.TEXT_PLAIN);
        return gsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getAllQueueUsageBetween(String startDate, String endDate) {
        String jsonUser = clientRequest(String.format(calvalusReportingWebServicesUrl.concat("/range-queue/%s/%s"), startDate, endDate), MediaType.TEXT_PLAIN);
        return gsonToUserInfo(jsonUser);
    }

    private List<UserInfo> gsonToUserInfo(String jsonUser) {
        if (jsonUser == null || jsonUser.contains(STATUS_FAILED)) {
            return null;
        }
        Gson gson = new Gson();
        Type mapType = new TypeToken<List<UserInfo>>() {
        }.getType();
        List<UserInfo> userInfoList = gson.fromJson(jsonUser, mapType);

        List<UserInfo> changUnit = new ArrayList<>();
        for (UserInfo userInfo : userInfoList) {
            changUnit.add(convertToUnits(userInfo));
        }

        return changUnit;
    }

    private UserInfo convertToUnits(UserInfo p) {
        String totalFileReadingMb = convertSize(p.getTotalFileReadingMb(), TO_GB);
        String totalFileWritingMb = convertSize(p.getTotalFileWritingMb(), TO_GB);
        String totalMemoryUsedMbs = convertSize(p.getTotalMemoryUsedMbs(), TO_GB * 3600);

        return new UserInfo(p.getJobsInDate(),
                            p.getJobsInQueue(),
                            p.getUser(),
                            p.getJobsProcessed(),
                            totalFileReadingMb,
                            totalFileWritingMb,
                            totalMemoryUsedMbs,
                            p.getTotalCpuTimeSpent(),
                            p.getTotalMap());
    }

    private String convertSize(String totalFileReadingMb, double size) {
        Number parse = null;
        try {
            NumberFormat instance = DecimalFormat.getInstance();
            parse = instance.parse(totalFileReadingMb);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return String.format("%.3f ", parse.longValue() / size);
    }

    private static String clientRequest(String uri, String textPlain) {
        Invocation.Builder builder = ClientBuilder.newClient().target(uri).request();
        Response response = builder.accept(textPlain).get();
        int status = response.getStatus();
        if (status >= HTTP_SUCCESSFUL_CODE_START && status < HTTP_SUCCESSFUL_CODE_END) {
            return builder.get(String.class);
        }
        return null;
    }
}
