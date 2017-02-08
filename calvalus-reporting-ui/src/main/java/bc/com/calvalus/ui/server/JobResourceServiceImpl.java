package bc.com.calvalus.ui.server;

import bc.com.calvalus.ui.client.JobResourcesService;
import bc.com.calvalus.ui.shared.UserInfo;
import bc.com.calvalus.ui.shared.UserInfoInDetails;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author muhammad.bc.
 */

public class JobResourceServiceImpl extends RemoteServiceServlet implements JobResourcesService {
    private static final Client client = ClientBuilder.newClient();
    public static final String STATUS_FAILED = "\"Status\": \"Failed\"";
    public static final String CALVALUS_REPORTING_WS_URL = "http://urbantep-test:9080/calvalus-reporting/reporting";
    public static final int HTTP_SUCCESSFUL_CODE_START = 200;
    public static final int HTTP_SUCCESSFUL_CODE_END = 300;

    public static final int TO_GB = 1024;
    public static final int FIRST_DAY = 1;
    public static final int FIRST_MONTH = 1;

    @Override
    public UserInfoInDetails getAllUserTodaySummary() {
        LocalDate now = LocalDate.now();
        List<UserInfo> allUserUsageSummaryBetween = getAllUserUsageSummaryBetween(now.toString(), now.toString());
        return new UserInfoInDetails(allUserUsageSummaryBetween, now.toString(), now.toString());
    }

    @Override
    public UserInfoInDetails getAllUserThisWeekSummary() {

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(endDate.getDayOfWeek().getValue());
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }

    @Override
    public UserInfoInDetails getAllUserLastWeekSummary() {
        LocalDate now = LocalDate.now();
        DayOfWeek dayOfWeek = now.getDayOfWeek();

        LocalDate endDate = now.minusDays(dayOfWeek.getValue());
        LocalDate startDate = endDate.minusDays(7);
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }

    @Override
    public UserInfoInDetails getAllUserThisMonthSummary() {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.withDayOfMonth(1);
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }

    @Override
    public UserInfoInDetails getAllUserLastMonthSummary() {
        LocalDate now = LocalDate.now();
        now = now.minusMonths(1);
        LocalDate startDate = now.withDayOfMonth(1);
        LocalDate endDate = now.withDayOfMonth(now.getMonth().maxLength());

        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }


    @Override
    public UserInfoInDetails getAllUserYesterdaySummary() {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(FIRST_DAY);
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }

    @Override
    public UserInfoInDetails getAllUserSummaryBetween(String startDate, String endDate) {
        List<UserInfo> allUserUsageSummaryBetween = getAllUserUsageSummaryBetween(startDate, endDate);
        return new UserInfoInDetails(allUserUsageSummaryBetween, startDate, endDate);
    }

    private List<UserInfo> getAllUserUsageSummaryBetween(String startDate, String endDate) {
        String jsonUser = clientRequest(String.format(CALVALUS_REPORTING_WS_URL.concat("/range/%s/%s"), startDate, endDate), MediaType.TEXT_PLAIN);
        return getGsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getGsonToUserInfo(String jsonUser) {
        if (jsonUser == null || jsonUser.contains(STATUS_FAILED)) {
            return null;
        }
        Gson gson = new Gson();
        Type mapType = new TypeToken<List<UserInfo>>() {
        }.getType();
        List<UserInfo> userInfoList = gson.fromJson(jsonUser, mapType);

        List<UserInfo> changUnit = new ArrayList<>();
        for (UserInfo userInfo : userInfoList) {
            changUnit.add(convertUnits(userInfo));
        }

        return changUnit;
    }

    private UserInfo convertUnits(UserInfo p) {
        return new UserInfo(p.getUser(),
                            p.getJobsProcessed(),
                            convertMBToGB(p.getTotalFileReadingMb(), TO_GB),
                            convertMBToGB(p.getTotalFileWritingMb(), TO_GB),
                            convertMBToGB(p.getTotalMemoryUsedMbs(), Math.pow(TO_GB, 2)),
                            p.getTotalCpuTimeSpent(),
                            p.getTotalMaps());
    }

    private String convertMBToGB(String totalFileReadingMb, double size) {
        Number parse = null;
        try {
            NumberFormat instance = DecimalFormat.getInstance();
            parse = instance.parse(totalFileReadingMb);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return String.format("%.4f ", parse.longValue() / size);
    }

    private static String clientRequest(String uri, String textPlain) {
        Invocation.Builder builder = client.target(uri).request();
        Response response = builder.accept(textPlain).get();
        int status = response.getStatus();
        if (status >= HTTP_SUCCESSFUL_CODE_START && status < HTTP_SUCCESSFUL_CODE_END) {
            return builder.get(String.class);
        }
        return null;
    }
}
