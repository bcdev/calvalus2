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
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;


/**
 * @author muhammad.bc.
 */
@Ignore
class WriteToLogTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    @After
    void tearDown() {
        folder.delete();
    }

    @Test
    public void writeAccoutMessageTemp() throws IOException {
        List<Message> messageList = Arrays.asList(
                createMessage("2017-02-02"),
                createMessage("2017-03-02"),
                createMessage("2017-04-05"));
        folder.create();
        File tempRootFile = folder.newFolder("test");

        System.out.println();
        String[] list = tempRootFile.list();

        assertEquals(list.length, 3);
        Message message = getMessage(new File(tempRootFile, list[0]));
        assertEquals(message.getHostName(),"brockmann");
        assertEquals(message.getStatus(),"PASS");
        assertEquals(message.getCompound().getId(),"02");
        assertEquals(message.getAccount(),null);

    }



    private Message getMessage(File file) throws IOException {
        FileReader fileReader = new FileReader(file);
        String readLine = new BufferedReader(fileReader).readLine();
        Gson gson = new Gson();
        return gson.fromJson(readLine, Message.class);
    }

    private Message createMessage(String date) {
        LocalDate parse = LocalDate.parse(date);
        Compound compound = new Compound("02", "test", "test-all", "host", parse.toString());
        return new Message("01", null, compound, null, "brockmann", Instant.now().toString(), "PASS");
    }


}