package com.bc.calvalus.code.de.sender;

import com.bc.calvalus.code.de.reader.JobDetail;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author muhammad.bc.
 */
public class FactoryProcessedMessage {

    public static ProcessedMessage[] createEmpty() {
        return new ProcessedMessage[]{new NullProcessedMessage()};
    }

    public static ProcessedMessage[] createProcessedMessage(List<JobDetail> jobDetails) {
        List<ProcessedMessage> collect = jobDetails.stream().map(p -> new ProcessedMessage(p))
                .collect(Collectors.toList());
        return collect.toArray(new ProcessedMessage[0]);
    }
}
