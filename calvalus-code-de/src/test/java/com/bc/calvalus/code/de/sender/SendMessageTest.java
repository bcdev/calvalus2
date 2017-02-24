package com.bc.calvalus.code.de.sender;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author muhammad.bc.
 */
@Ignore
public class SendMessageTest {
    @Test
    public void testSendMessage() throws Exception {
        ProcessedMessage[] processedMessage = FactoryProcessedMessage.createEmpty();
        SendMessage sendMessage = new SendMessage(processedMessage);
        Boolean aBoolean = sendMessage.send();
        assertTrue(aBoolean);
    }

}