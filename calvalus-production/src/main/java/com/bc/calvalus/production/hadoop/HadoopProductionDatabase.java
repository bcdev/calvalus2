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
            boolean outputStaging = Boolean.parseBoolean(tokens[2]);
            int[] offpt = new int[]{3};
            JobID[] jobIDs = decodeJobIdsTSV(tokens, offpt);
            ProductionRequest productionRequest = decodeProductionRequestTSV(tokens, offpt);
            ProductionStatus productionStatus = decodeProductionStatusTSV(tokens, offpt);
            ProductionStatus stagingStatus = decodeProductionStatusTSV(tokens, offpt);

            HadoopProduction hadoopProduction = new HadoopProduction(id, name,
                                                                     outputStaging, jobIDs,
                                                                     productionRequest);

            hadoopProduction.setProcessingStatus(productionStatus);
            hadoopProduction.setStagingStatus(stagingStatus);
            addProduction(hadoopProduction);
        }
    }

    public synchronized void store(PrintWriter writer) {
        for (HadoopProduction production : productionsList) {
            writer.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\tEOR\n",
                          production.getId(),
                          production.getName(),
                          production.isOutputStaging(),
                          encodeJobIdsTSV(production.getJobIds()),
                          encodeProductionRequestTSV(production.getProductionRequest()),
                          encodeProductionStatusTSV(production.getProcessingStatus()),
                          encodeProductionStatusTSV(production.getStagingStatus())
            );
        }
    }


    private static JobID[] decodeJobIdsTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        int numJobs = Integer.parseInt(tokens[off++]);
        JobID[] jobIds = new JobID[numJobs];
        for (int i = 0; i < numJobs; i++) {
            jobIds[i] = JobID.forName(tokens[off++]);
        }
        offpt[0] = off;
        return jobIds;
    }

    private static String encodeJobIdsTSV(JobID[] jobIds) {
        StringBuilder sb = new StringBuilder();
        sb.append(jobIds.length);
        for (JobID jobId : jobIds) {
            sb.append("\t");
            sb.append(jobId.toString());
        }
        return sb.toString();
    }

    static String encodeProductionRequestTSV(ProductionRequest productionRequest) {
        Map<String, String> productionParameters = productionRequest.getProductionParameters();

        StringBuilder sb = new StringBuilder();
        sb.append(productionRequest.getProductionType());
        sb.append("\t");
        sb.append(productionParameters.size());
        for (Map.Entry<String, String> entry : productionParameters.entrySet()) {
            sb.append("\t");
            sb.append(encodeTSV(entry.getKey()));
            sb.append("\t");
            sb.append(encodeTSV(entry.getValue()));
        }
        return sb.toString();
    }

    private static ProductionRequest decodeProductionRequestTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        String productionType = decodeTSV(tokens[off++]);
        int numParams = Integer.parseInt(tokens[off++]);
        Map<String, String> productionParameters = new HashMap<String, String>();
        for (int i = 0; i < numParams; i++) {
            String name = decodeTSV(tokens[off++]);
            String value = decodeTSV(tokens[off++]);
            productionParameters.put(name, value);
        }
        offpt[0] = off;
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

    static ProductionStatus decodeProductionStatusTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        ProductionState productionState = ProductionState.valueOf(tokens[off++]);
        float progress = Float.parseFloat(tokens[off++]);
        String message = encodeTSV(tokens[off++]);
        offpt[0] = off;
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
