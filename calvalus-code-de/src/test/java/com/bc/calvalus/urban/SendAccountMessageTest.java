package com.bc.calvalus.urban;

import com.bc.calvalus.urban.account.Compound;
import com.bc.calvalus.urban.account.Message;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author muhammad.bc.
 */

class SendAccountMessageTest {
    @BeforeAll
    static void setUp() throws IOException {
        PropertiesWrapper.loadConfigFile("urbanTEP.properties");
    }

    @Test
    void sendAccoundMessage() {
        List<Message> messageList = Arrays.asList(
                createMessage("2017-02-02"),
                createMessage("2017-04-05"));

        SendAccountMessage sendAccountMessage = SendAccountMessage.getInstance();
        sendAccountMessage.send(messageList);
    }
    private Message createMessage(String date) {
        LocalDate parse = LocalDate.parse(date);
        Date dateFrom = Date.from(parse.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
        Compound compound = new Compound("02", "test", "test-all", "host", dateFrom);
        return new Message("01", null, compound, null, "brockmann", Instant.now().toString(), "PASS");
    }
}