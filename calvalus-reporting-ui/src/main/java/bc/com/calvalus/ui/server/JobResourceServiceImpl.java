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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
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
    public static final int HTTP_SUCCESSFULL_START = 200;
    public static final int HTTP_SUCCESSFULL_END = 300;

    public static final int MAGE_UNIT = 1024;

    @Override
    public UserInfoInDetails getAllUserTodaySummary() {
        LocalDate now = LocalDate.now();
        List<UserInfo> allUserUsageSummaryBetween = getAllUserUsageSummaryBetween(now.toString(), now.toString());
        return new UserInfoInDetails(allUserUsageSummaryBetween, now.toString(), now.toString());
    }

    @Override
    public UserInfoInDetails getAllUserWeekAgoSummary() {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusWeeks(1);
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }

    @Override
    public UserInfoInDetails getAllUserMonthAgoSummary() {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusMonths(1);
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }


    @Override
    public UserInfoInDetails getAllUserYesterdaySummary() {
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(1);
        return getAllUserSummaryBetween(startDate.toString(), endDate.toString());
    }

    @Override
    public UserInfoInDetails getAllUserSummaryBetween(String startDate, String endDate) {
        List<UserInfo> allUserUsageSummaryBetween = getAllUserUsageSummaryBetween(startDate, endDate);
        return new UserInfoInDetails(allUserUsageSummaryBetween, startDate, endDate);
    }


    private List<UserInfo> getUserUsageSummary(String name, String startDate, String endDate) {
        String jsonUser = clientRequest(String.format(CALVALUS_REPORTING_WS_URL.concat("/%s/range/%s/%s"), name, startDate, endDate), MediaType.TEXT_PLAIN);
        if (jsonUser.contains(STATUS_FAILED)) {
            return null;
        }
        Gson gson = new Gson();
        UserInfo userInfo = gson.fromJson(jsonUser, UserInfo.class);
        return Collections.singletonList(convertUnits(userInfo));
    }

    private List<UserInfo> getUserUsageSummary(String userName) {
        String jsonUser = clientRequest(String.format(CALVALUS_REPORTING_WS_URL.concat("/%s"), userName), MediaType.APPLICATION_JSON);
        return getGsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getAllUserUsageSummaryBetween(String startDate, String endDate) {
        String jsonUser = clientRequest(String.format(CALVALUS_REPORTING_WS_URL.concat("/range/%s/%s"), startDate, endDate), MediaType.TEXT_PLAIN);
        return getGsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getAllUserSummaryForAMonth() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String startDate = LocalDate.now().atStartOfDay().minusMonths(1).format(formatter);
        String endOfDay = LocalDate.now().atStartOfDay().format(formatter);
        String responseJson = String.format(CALVALUS_REPORTING_WS_URL.concat("/range/%s/%s"), startDate, endOfDay);
        String jsonUser = clientRequest(responseJson, MediaType.TEXT_PLAIN);
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
                            convertMBToGB(p.getTotalFileReadingMb()),
                            convertMBToGB(p.getTotalFileWritingMb()),
                            convertMBToGB(p.getTotalMemoryUsedMbs()),
                            p.getTotalCpuTimeSpent(),
                            p.getTotalMaps());
    }

    private String convertMBToGB(String totalFileReadingMb) {
        Number parse = null;
        try {
            NumberFormat instance = DecimalFormat.getInstance();
            parse = instance.parse(totalFileReadingMb);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return String.format("%.4f ", parse.longValue() / 1024d);
    }

    public String formatSize(long i) {
        double pow = Math.pow(MAGE_UNIT, 2);
        if (i <= pow) {
            return String.format("%.4f GBs", (i / 1024d));
        } else {
            return String.format("%.4f TBs", (i / pow));
        }
    }

    private static String clientRequest(String uri, String textPlain) {
        Invocation.Builder builder = client.target(uri).request();
        Response response = builder.accept(textPlain).get();
        int status = response.getStatus();
        if (status >= HTTP_SUCCESSFULL_START && status < HTTP_SUCCESSFULL_END) {
            return builder.get(String.class);
        }
        return null;
    }
}
