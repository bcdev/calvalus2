package com.bc.calvalus.urban;

import com.bc.calvalus.urban.account.Compound;
import com.bc.calvalus.urban.account.Message;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;


/**
 * @author muhammad.bc.
 */
class WriteToLogTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    void writeAccoutMessageTemp() throws IOException {
/*        List<Message> messageList = Arrays.asList(
                createMessage("2017-02-02"),
                createMessage("2017-03-02"),
                createMessage("2017-04-05"));
        folder.create();
        File tempRootFile = folder.newFolder("test");
        SendAccountMessage.WriteToLog writeToLog = new SendAccountMessage.WriteToLog(messageList, tempRootFile.toPath().toString());
        System.out.println();
        String[] list = tempRootFile.list();

        assertEquals(list.length, 3);
        Message message = getMessage(new File(tempRootFile, list[0]));
        assertEquals(message.getHostName(),"brockmann");
        assertEquals(message.getStatus(),"PASS");
        assertEquals(message.getCompound().getId(),"02");
        assertEquals(message.getAccount(),null);*/

    }

    @AfterEach
    void tearDown() {
        folder.delete();
    }

    private Message getMessage(File file) throws IOException {
        FileReader fileReader = new FileReader(file);
        String readLine = new BufferedReader(fileReader).readLine();
        Gson gson = new Gson();
        return gson.fromJson(readLine, Message.class);
    }

    private Message createMessage(String date) {
        LocalDate parse = LocalDate.parse(date);
        Date dateFrom = Date.from(parse.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
        Compound compound = new Compound("02", "test", "test-all", "host", dateFrom);
        return new Message("01", null, compound, null, "brockmann", Instant.now().toString(), "PASS");
    }
}