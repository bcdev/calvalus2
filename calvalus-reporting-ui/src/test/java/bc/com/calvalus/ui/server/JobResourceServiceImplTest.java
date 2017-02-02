package bc.com.calvalus.ui.server;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author muhammad.bc.
 */
public class JobResourceServiceImplTest {

    @Test
    public void testName() throws Exception {
        Type mapType = new TypeToken<List<UserInfo>>() {
        }.getType();

        List<UserInfo> userInfoList = new Gson().fromJson(json, mapType);
        assertEquals(userInfoList.size(), 10);

        UserInfo userInfo = userInfoList.get(0);
        assertEquals(userInfo.getUser(), "hucke");
        assertEquals(userInfo.getJobsProcessed(), "1");
        assertEquals(userInfo.getTotalCpuTimeSpent(), "01:43:26");
        assertEquals(userInfo.getTotalVcoresUsed(), "8,224");
        assertEquals(userInfo.getTotalMemoryUsedMbs(), "42,110,858");
        assertEquals(userInfo.getTotalFileReadingMb(), "36,293");
    }

    String json = "[\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.0\",\n" +
            "    \"totalFileReadingMb\": \"36,293\",\n" +
            "    \"cpuUsagePrice\": \"0.0\",\n" +
            "    \"diskUsageprice\": \"0.39\",\n" +
            "    \"totalPrice\": \"0.39\",\n" +
            "    \"totalFileWritingMb\": \"47\",\n" +
            "    \"totalMemoryUsedMbs\": \"42,110,858\",\n" +
            "    \"totalCpuTimeSpent\": \"01:43:26\",\n" +
            "    \"jobsProcessed\": \"1\",\n" +
            "    \"user\": \"hucke\",\n" +
            "    \"totalVcoresUsed\": \"8,224\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.01\",\n" +
            "    \"totalFileReadingMb\": \"396,751\",\n" +
            "    \"cpuUsagePrice\": \"0.01\",\n" +
            "    \"diskUsageprice\": \"4.33\",\n" +
            "    \"totalPrice\": \"4.35\",\n" +
            "    \"totalFileWritingMb\": \"212\",\n" +
            "    \"totalMemoryUsedMbs\": \"122,599,782\",\n" +
            "    \"totalCpuTimeSpent\": \"03:55:39\",\n" +
            "    \"jobsProcessed\": \"1\",\n" +
            "    \"user\": \"calbfgtest\",\n" +
            "    \"totalVcoresUsed\": \"23,945\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.0\",\n" +
            "    \"totalFileReadingMb\": \"13,298\",\n" +
            "    \"cpuUsagePrice\": \"0.0\",\n" +
            "    \"diskUsageprice\": \"0.18\",\n" +
            "    \"totalPrice\": \"0.18\",\n" +
            "    \"totalFileWritingMb\": \"3,392\",\n" +
            "    \"totalMemoryUsedMbs\": \"16,212,715\",\n" +
            "    \"totalCpuTimeSpent\": \"00:10:52\",\n" +
            "    \"jobsProcessed\": \"2\",\n" +
            "    \"user\": \"jessica\",\n" +
            "    \"totalVcoresUsed\": \"6,332\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.71\",\n" +
            "    \"totalFileReadingMb\": \"3,684,062\",\n" +
            "    \"cpuUsagePrice\": \"2.11\",\n" +
            "    \"diskUsageprice\": \"40.51\",\n" +
            "    \"totalPrice\": \"43.33\",\n" +
            "    \"totalFileWritingMb\": \"19,806\",\n" +
            "    \"totalMemoryUsedMbs\": \"11,721,905,832\",\n" +
            "    \"totalCpuTimeSpent\": \"788:25:52\",\n" +
            "    \"jobsProcessed\": \"24\",\n" +
            "    \"user\": \"mortimer\",\n" +
            "    \"totalVcoresUsed\": \"5,751,156\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.0\",\n" +
            "    \"totalFileReadingMb\": \"7,431\",\n" +
            "    \"cpuUsagePrice\": \"0.0\",\n" +
            "    \"diskUsageprice\": \"0.08\",\n" +
            "    \"totalPrice\": \"0.08\",\n" +
            "    \"totalFileWritingMb\": \"38\",\n" +
            "    \"totalMemoryUsedMbs\": \"1,323,451\",\n" +
            "    \"totalCpuTimeSpent\": \"00:06:57\",\n" +
            "    \"jobsProcessed\": \"2\",\n" +
            "    \"user\": \"carsten\",\n" +
            "    \"totalVcoresUsed\": \"861\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.07\",\n" +
            "    \"totalFileReadingMb\": \"651,344\",\n" +
            "    \"cpuUsagePrice\": \"0.39\",\n" +
            "    \"diskUsageprice\": \"11.86\",\n" +
            "    \"totalPrice\": \"12.32\",\n" +
            "    \"totalFileWritingMb\": \"433,388\",\n" +
            "    \"totalMemoryUsedMbs\": \"1,089,363,842\",\n" +
            "    \"totalCpuTimeSpent\": \"191:02:02\",\n" +
            "    \"jobsProcessed\": \"4\",\n" +
            "    \"user\": \"aledang\",\n" +
            "    \"totalVcoresUsed\": \"1,063,830\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"17.29\",\n" +
            "    \"totalFileReadingMb\": \"19,450,358\",\n" +
            "    \"cpuUsagePrice\": \"20.68\",\n" +
            "    \"diskUsageprice\": \"369.79\",\n" +
            "    \"totalPrice\": \"407.76\",\n" +
            "    \"totalFileWritingMb\": \"14,359,691\",\n" +
            "    \"totalMemoryUsedMbs\": \"285,892,069,738\",\n" +
            "    \"totalCpuTimeSpent\": \"18110:48:23\",\n" +
            "    \"jobsProcessed\": \"1500\",\n" +
            "    \"user\": \"martin\",\n" +
            "    \"totalVcoresUsed\": \"56,316,578\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"1.78\",\n" +
            "    \"totalFileReadingMb\": \"37,826,596\",\n" +
            "    \"cpuUsagePrice\": \"10.33\",\n" +
            "    \"diskUsageprice\": \"415.15\",\n" +
            "    \"totalPrice\": \"427.26\",\n" +
            "    \"totalFileWritingMb\": \"130,388\",\n" +
            "    \"totalMemoryUsedMbs\": \"29,343,671,531\",\n" +
            "    \"totalCpuTimeSpent\": \"6426:17:03\",\n" +
            "    \"jobsProcessed\": \"1381\",\n" +
            "    \"user\": \"cvop\",\n" +
            "    \"totalVcoresUsed\": \"28,138,453\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.37\",\n" +
            "    \"totalFileReadingMb\": \"11,343,903\",\n" +
            "    \"cpuUsagePrice\": \"0.52\",\n" +
            "    \"diskUsageprice\": \"257.88\",\n" +
            "    \"totalPrice\": \"258.77\",\n" +
            "    \"totalFileWritingMb\": \"12,234,642\",\n" +
            "    \"totalMemoryUsedMbs\": \"6,080,378,193\",\n" +
            "    \"totalCpuTimeSpent\": \"501:19:18\",\n" +
            "    \"jobsProcessed\": \"626\",\n" +
            "    \"user\": \"thomas\",\n" +
            "    \"totalVcoresUsed\": \"1,428,451\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"memoryUsagePrice\": \"0.0\",\n" +
            "    \"totalFileReadingMb\": \"2,194\",\n" +
            "    \"cpuUsagePrice\": \"0.0\",\n" +
            "    \"diskUsageprice\": \"0.02\",\n" +
            "    \"totalPrice\": \"0.02\",\n" +
            "    \"totalFileWritingMb\": \"55\",\n" +
            "    \"totalMemoryUsedMbs\": \"1,194,891\",\n" +
            "    \"totalCpuTimeSpent\": \"00:07:32\",\n" +
            "    \"jobsProcessed\": \"5\",\n" +
            "    \"user\": \"hibbett\",\n" +
            "    \"totalVcoresUsed\": \"902\"\n" +
            "  }\n" +
            "]";
}