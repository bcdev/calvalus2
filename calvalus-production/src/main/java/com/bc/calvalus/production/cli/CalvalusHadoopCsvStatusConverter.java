package com.bc.calvalus.production.cli;

import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopCsvStatusConverter extends CalvalusHadoopStatusConverter {

    public CalvalusHadoopCsvStatusConverter(CalvalusHadoopConnection hadoopConnection) {
        super(hadoopConnection);
    }

    @Override
    public void accumulateJobStatus(String id, String status, double progress, String message, StringBuilder accu) {
        accu.append(id);
        accu.append(",");
        accu.append(status);
        accu.append(",");
        accu.append(String.format("%5.3f", progress));
        if (message != null) {
            accu.append(",");
            accu.append(message);
        }
        accu.append("\n");
    }

    public void accumulateJobConfiguration(String id, Iterable<Map.Entry<String, String>> configuration, StringBuilder accu) {
        accu.append("id");
        accu.append(",");
        accu.append(id);
        accu.append("\n");
        for (Map.Entry<String, String> entry : configuration) {
            accu.append(entry.getKey());
            accu.append(",");
            accu.append(entry.getValue());
            accu.append("\n");
        }
    }

    @Override
    public void initialiseJobStatus(StringBuilder accu) {}

    @Override
    protected void separateJobStatus(StringBuilder accu) {}

    @Override
    public void finaliseJobStatus(StringBuilder accu) {}
}
