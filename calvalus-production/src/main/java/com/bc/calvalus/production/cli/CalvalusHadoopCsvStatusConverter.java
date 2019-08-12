package com.bc.calvalus.production.cli;

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

    @Override
    public void initialiseJobStatus(StringBuilder accu) {}

    @Override
    protected void separateJobStatus(StringBuilder accu) {}

    @Override
    public void finaliseJobStatus(StringBuilder accu) {}
}
