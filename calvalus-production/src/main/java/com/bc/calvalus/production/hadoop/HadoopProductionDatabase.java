package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.mapreduce.JobID;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A database for productions.
 *
 * @author Norman
 */
class HadoopProductionDatabase {

    private final List<HadoopProduction> productionsList;
    private final Map<String, HadoopProduction> productionsMap;

    public HadoopProductionDatabase() {
        this.productionsList = new ArrayList<HadoopProduction>();
        this.productionsMap = new HashMap<String, HadoopProduction>();
    }

    public synchronized void addProduction(HadoopProduction hadoopProduction) {
        productionsList.add(hadoopProduction);
        productionsMap.put(hadoopProduction.getId(), hadoopProduction);
    }

    public synchronized void removeProduction(HadoopProduction hadoopProduction) {
        productionsList.remove(hadoopProduction);
        productionsMap.remove(hadoopProduction.getId());
    }

    public synchronized HadoopProduction[] getProductions() {
        return productionsList.toArray(new HadoopProduction[productionsList.size()]);
    }

    public synchronized HadoopProduction getProduction(String productionId) {
        return productionsMap.get(productionId);
    }

    public synchronized void load(File databaseFile) throws IOException {
        if (databaseFile.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(databaseFile));
            try {
                load(reader);
            } finally {
                reader.close();
            }
        }
    }

    public synchronized void store(File databaseFile) throws IOException {
        // System.out.printf("%s: Storing %d production(s) to %s%n", this, productionsList.size(), databaseFile.getAbsolutePath());
        File bakFile = new File(databaseFile.getParentFile(), databaseFile.getName() + ".bak");
        bakFile.delete();
        databaseFile.renameTo(bakFile);
        PrintWriter writer = new PrintWriter(new FileWriter(databaseFile));
        try {
            store(writer);
        } finally {
            writer.close();
        }
    }

    public synchronized void load(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\t");
            String id = tokens[0];
            String name = tokens[1];
            String jobId = tokens[2];
            boolean outputStaging = Boolean.parseBoolean(tokens[3]);
            ProductionStatus productionStatus = decodeProductionStatusTSV(tokens, 4);
            ProductionStatus stagingStatus = decodeProductionStatusTSV(tokens, 4 + 3);
            ProductionRequest productionRequest = decodeProductionRequestTSV(tokens, 4 + 3 + 3);

            HadoopProduction hadoopProduction = new HadoopProduction(id, name,
                                                                     JobID.forName(jobId),
                                                                     outputStaging, productionRequest);
            hadoopProduction.setProcessingStatus(productionStatus);
            hadoopProduction.setStagingStatus(stagingStatus);
            addProduction(hadoopProduction);
        }
    }

    public synchronized void store(PrintWriter writer) {
        for (HadoopProduction production : productionsList) {
            writer.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                          production.getId(),
                          production.getName(),
                          production.getJobId(),
                          production.isOutputStaging(),
                          encodeProductionStatusTSV(production.getProcessingStatus()),
                          encodeProductionStatusTSV(production.getStagingStatus()),
                          encodeProductionRequestTSV(production.getProductionRequest()));
        }
    }

    static String encodeProductionRequestTSV(ProductionRequest productionRequest) {
        StringBuilder sb = new StringBuilder(productionRequest.getProductionType());
        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        Set<Map.Entry<String, String>> entries = productionParameters.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            sb.append("\t");
            sb.append(encodeTSV(entry.getKey()));
            sb.append("\t");
            sb.append(encodeTSV(entry.getValue()));
        }
        return sb.toString();
    }

    private static ProductionRequest decodeProductionRequestTSV(String[] tokens, int off) {
        String productionType = decodeTSV(tokens[off]);
        Map<String, String> productionParameters = new HashMap<String, String>();
        for (int i = off + 1; i < tokens.length; i += 2) {
            productionParameters.put(decodeTSV(tokens[i]), decodeTSV(tokens[i + 1]));
        }
        return new ProductionRequest(productionType, productionParameters);
    }

    static String encodeProductionStatusTSV(ProductionStatus productionStatus) {
        StringBuilder sb = new StringBuilder();
        sb.append(productionStatus.getState());
        sb.append("\t");
        sb.append(productionStatus.getProgress());
        sb.append("\t");
        sb.append(encodeTSV(productionStatus.getMessage()));
        return sb.toString();
    }

    static ProductionStatus decodeProductionStatusTSV(String[] tokens, int off) {
        ProductionState productionState = ProductionState.valueOf(tokens[off]);
        float progress = Float.parseFloat(tokens[off + 1]);
        String message = encodeTSV(tokens[off + 2]);
        return new ProductionStatus(productionState, progress, message);
    }

    // TSV = tab separated characters
    private static String encodeTSV(String value) {
        return value.replace("\n", "\\n").replace("\t", "\\t");
    }

    // TSV = tab separated characters
    private static String decodeTSV(String tsv) {
        return tsv.replace("\\n", "\n").replace("\\t", "\t");
    }
}
