package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.reporting.urban.account.Account;
import com.bc.calvalus.reporting.urban.account.Any;
import com.bc.calvalus.reporting.urban.account.Compound;
import com.bc.calvalus.reporting.urban.account.Message;
import com.bc.calvalus.reporting.urban.account.Quantity;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author muhammad.bc.
 */
public class SendWriteMessageTest {
    public static final String EXPECTED_MESSAGE = "{\"id\":\"job_1234\",\"account\":{\"platform\":\"bla\",\"userName\":\"wooo\",\"ref\":\"what\"},\"compound\":{\"id\":\"1234\",\"name\":\"sometime\",\"type\":\"lost\",\"any\":{\"uri\":\"2017-05-03\"}},\"quantity\":[{\"id\":\"BYTE_WRITTEN\",\"value\":285998},{\"id\":\"PROC_INSTANCE\",\"value\":1},{\"id\":\"PHYSICAL_MEMORY_BYTES\",\"value\":27810},{\"id\":\"CPU_MILLISECONDS\",\"value\":27810},{\"id\":\"NUM_REQ\",\"value\":0}],\"hostName\":\"host\",\"timeStamp\":\"2017-05-03\",\"status\":\"COMPLETE\"}";

    @Test
    public void testCreateMessageJsonFormat() throws Exception {
        Message message = createmessage();
        Account account = message.getAccount();
        Compound compound = message.getCompound();

        assertNotNull(message);
        assertNotNull(account);
        assertNotNull(compound);

        String toJson = message.toJson();
        assertNotNull(toJson);
        assertEquals(EXPECTED_MESSAGE, toJson);

    }

    private Message createmessage() {
        Account account = new Account("bla", "wooo", "what");
        String timestamp = "2017-05-03";
        Compound compound = new Compound("1234", "sometime", "lost", new Any(timestamp));

        List<Quantity> quantityList = Arrays.asList(
                new Quantity("BYTE_WRITTEN", 285998l),
                new Quantity("PROC_INSTANCE", 1l),
                new Quantity("PHYSICAL_MEMORY_BYTES", 27810l),
                new Quantity("CPU_MILLISECONDS", 27810l),
                new Quantity("NUM_REQ", 0l));

        Message message = new Message("job_1234",
                                      account,
                                      compound,
                                      quantityList,
                                      "host",
                                      timestamp,
                                      "COMPLETE");

        return message;
    }
}