package bc.com.calvalus.ui.server;

import bc.com.calvalus.ui.client.JobResourcesService;
import bc.com.calvalus.ui.shared.UserInfo;
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


    @Override
    public List<String> getAllUserName() {
        List<String> nameList = new ArrayList<>();
        String jsonUser = clientRequest("http://urbantep-test:9080/calvalus-reporting/reporting/all/users", MediaType.APPLICATION_JSON);
        List<UserInfo> gsonToUserInfo = getGsonToUserInfo(jsonUser);
        gsonToUserInfo.forEach(p -> nameList.add(p.getUser()));
        return nameList;
    }

    @Override
    public List<UserInfo> getAllUserSummary(String userName, String startDate, String endDate) {
        if (userName == null && startDate == null && endDate == null) {
            return getAllUserSummaryForAMonth();
        }
        if (userName != null && (startDate == null && endDate == null)) {
            return getUserUsageSummary(userName);
        }

        if (userName != null && startDate != null && endDate != null) {
            if (userName.equalsIgnoreCase("all")) {
                return getAllUserUsageSummaryBetween(startDate, endDate);
            }
            return getUserUsageSummary(userName, startDate, endDate);
        }

        return null;
    }


    private List<UserInfo> getUserUsageSummary(String name, String startDate, String endDate) {
        String jsonUser = clientRequest(String.format("http://urbantep-test:9080/calvalus-reporting/reporting/%s/range/%s/%s", name, startDate, endDate), MediaType.TEXT_PLAIN);
        if (jsonUser.contains(STATUS_FAILED)) {
            return null;
        }
        Gson gson = new Gson();
        UserInfo userInfo = gson.fromJson(jsonUser, UserInfo.class);
        return Collections.singletonList(convertUnits(userInfo));
    }

    private List<UserInfo> getUserUsageSummary(String userName) {
        String jsonUser = clientRequest(String.format("http://urbantep-test:9080/calvalus-reporting/reporting/%s", userName), MediaType.APPLICATION_JSON);
        return getGsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getAllUserUsageSummaryBetween(String startDate, String endDate) {
        String jsonUser = clientRequest(String.format("http://urbantep-test:9080/calvalus-reporting/reporting/range/%s/%s", startDate, endDate), MediaType.TEXT_PLAIN);
        return getGsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getAllUserSummaryForAMonth() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String endOfDay = LocalDate.now().atStartOfDay().format(formatter);
        String startDate = LocalDate.now().atStartOfDay().minusMonths(1).format(formatter);
        String format = String.format("http://urbantep-test:9080/calvalus-reporting/reporting/range/%s/%s", startDate, endOfDay);
        String jsonUser = clientRequest(format, MediaType.TEXT_PLAIN);
        return getGsonToUserInfo(jsonUser);
    }

    private List<UserInfo> getGsonToUserInfo(String jsonUser) {
        if (jsonUser.contains(STATUS_FAILED)) {
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
                            p.getTotalVcoresUsed());
    }

    private String convertMBToGB(String totalFileReadingMb) {
        Number parse = null;
        try {
            NumberFormat instance = DecimalFormat.getInstance();
            parse = instance.parse(totalFileReadingMb);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return String.format("%.4f", parse.doubleValue() / (1024));
    }

    private static String clientRequest(String uri, String textPlain) {
        Invocation.Builder builder = client.target(uri).request();
        Response response = builder.accept(textPlain).get();
        int status = response.getStatus();
        if (status >= 200 && status < 300) {
            return builder.get(String.class);
        }
        return null;
    }
}
