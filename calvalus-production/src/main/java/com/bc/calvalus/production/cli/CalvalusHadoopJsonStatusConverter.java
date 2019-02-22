package com.bc.calvalus.production.cli;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopJsonStatusConverter extends CalvalusHadoopStatusConverter {

    public CalvalusHadoopJsonStatusConverter(CalvalusHadoopConnection hadoopConnection) {
        super(hadoopConnection);
    }

    @Override
    public void accumulateJobStatus(String id, String status, double progress, String message, StringBuilder accu) {
        accu.append("\"");
        accu.append(id);
        accu.append("\": {\"status\": \"");
        accu.append(status);
        accu.append("\", \"progress\": ");
        accu.append(String.format("%5.3f", progress));
        if (message != null) {
            accu.append(", \"message\": \"");
            accu.append(message);
            accu.append("\"");
        }
        accu.append("}");
    }

    @Override
    public void initialiseJobStatus(StringBuilder accu) {
        accu.append("{");
    }

    @Override
    protected void separateJobStatus(StringBuilder accu) {
        if (accu.length() > 1) {
            accu.append(", ");
        }
    }

    @Override
    public void finaliseJobStatus(StringBuilder accu) {
        if (accu.length() > 1) {
            accu.append(" ");
        }
        accu.append("}\n");
    }
}
