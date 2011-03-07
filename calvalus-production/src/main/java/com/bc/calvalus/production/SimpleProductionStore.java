package com.bc.calvalus.production;

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
 * A plain text database for productions.
 *
 * @author Norman
 */
public class SimpleProductionStore implements ProductionStore {

    public static final File PRODUCTIONS_DB_FILE = new File("calvalus-productions-db.csv");

    private final List<Production> productionsList;
    private final Map<String, Production> productionsMap;
    private final JobIdFormat idFormat;

    public SimpleProductionStore() {
        this(new JobIdFormat() {
            @Override
            public String format(Object jobId) {
                return jobId.toString();
            }

            @Override
            public Object parse(String text) {
                return text;
            }
        });
    }

    public SimpleProductionStore(JobIdFormat idFormat) {
        this.idFormat = idFormat;
        this.productionsList = new ArrayList<Production>();
        this.productionsMap = new HashMap<String, Production>();
    }

    @Override
    public synchronized void addProduction(Production production) {
        productionsList.add(production);
        productionsMap.put(production.getId(), production);
    }

    @Override
    public synchronized void removeProduction(Production production) {
        productionsList.remove(production);
        productionsMap.remove(production.getId());
    }

    @Override
    public synchronized Production[] getProductions() {
        return productionsList.toArray(new Production[productionsList.size()]);
    }

    @Override
    public synchronized Production getProduction(String productionId) {
        return productionsMap.get(productionId);
    }

    @Override
    public synchronized void load() throws IOException {
        load(PRODUCTIONS_DB_FILE);
    }

    @Override
    public synchronized void store() throws IOException {
        store(PRODUCTIONS_DB_FILE);
    }

    private void load(File databaseFile) throws IOException {
        if (databaseFile.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(databaseFile));
            try {
                load(reader);
            } finally {
                reader.close();
            }
        }
    }

    private void store(File databaseFile) throws IOException {
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

    void load(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\t");
            String id = decodeTSV(tokens[0]);
            String name = decodeTSV(tokens[1]);
            String user = decodeTSV(tokens[2]);
            boolean outputStaging = Boolean.parseBoolean(tokens[3]);
            int[] offpt = new int[]{4};
            Object[] jobIDs = decodeJobIdsTSV(tokens, offpt);
            ProductionRequest productionRequest = decodeProductionRequestTSV(tokens, offpt);
            ProductionStatus productionStatus = decodeProductionStatusTSV(tokens, offpt);
            ProductionStatus stagingStatus = decodeProductionStatusTSV(tokens, offpt);

            Production hadoopProduction = new Production(id, name, user,
                                                         outputStaging, jobIDs,
                                                         productionRequest);

            hadoopProduction.setProcessingStatus(productionStatus);
            hadoopProduction.setStagingStatus(stagingStatus);
            addProduction(hadoopProduction);
        }
    }

    void store(PrintWriter writer) {
        for (Production production : productionsList) {
            writer.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tEoR\n",
                          encodeTSV(production.getId()),
                          encodeTSV(production.getName()),
                          encodeTSV(production.getUser()),
                          production.isOutputStaging(),
                          encodeJobIdsTSV(production.getJobIds()),
                          encodeProductionRequestTSV(production.getProductionRequest()),
                          encodeProductionStatusTSV(production.getProcessingStatus()),
                          encodeProductionStatusTSV(production.getStagingStatus()));
        }
    }


    private Object[] decodeJobIdsTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        int numJobs = Integer.parseInt(tokens[off++]);
        Object[] jobIds = new Object[numJobs];
        for (int i = 0; i < numJobs; i++) {
            jobIds[i] = idFormat.parse(decodeTSV(tokens[off++]));
        }
        offpt[0] = off;
        return jobIds;
    }

    private String encodeJobIdsTSV(Object[] jobIds) {
        StringBuilder sb = new StringBuilder();
        sb.append(jobIds.length);
        for (Object jobId : jobIds) {
            sb.append("\t");
            sb.append(encodeTSV(idFormat.format(jobId)));
        }
        return sb.toString();
    }

    String encodeProductionRequestTSV(ProductionRequest productionRequest) {
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
