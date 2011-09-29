package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A plain text database for productions.
 *
 * @author Norman
 */
public class CsvProductionStore implements ProductionStore {

    private final File databaseFile;
    private final List<Production> productionsList;
    private final Map<String, Production> productionsMap;
    private final ProcessingService processingService;

    public CsvProductionStore(ProcessingService processingService, File databaseFile) {
        if (processingService == null) {
            throw new NullPointerException("processingService");
        }
        if (databaseFile == null) {
            throw new NullPointerException("databaseFile");
        }
        this.databaseFile = databaseFile;
        this.processingService = processingService;
        this.productionsList = new ArrayList<Production>();
        this.productionsMap = new HashMap<String, Production>();
    }

    @Override
    public synchronized void addProduction(Production production) {
        productionsList.add(production);
        productionsMap.put(production.getId(), production);
    }

    @Override
    public synchronized void removeProduction(String  productionId) {
        productionsList.remove(productionId);
        productionsMap.remove(productionId);
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
    public synchronized void update() throws ProductionException {
        load(databaseFile);
    }

    @Override
    public synchronized void persist() throws ProductionException {
        try {
            store(databaseFile);
        } catch (IOException e) {
            String message = String.format("Failed to store productions in: %s\n%s",
                                           databaseFile.getPath(), e.getMessage());
            throw new ProductionException(message, e);
        }
    }

    private void load(File databaseFile) throws ProductionException {
        if (databaseFile.exists()) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(databaseFile));
                try {
                    load(reader);
                } finally {
                    reader.close();
                }
            } catch (IOException e) {
                String message = String.format("Failed to load production store from: %s\n%s",
                                               databaseFile.getPath(), e.getMessage());
                throw new ProductionException(message, e);
            }
        }
    }

    @Override
    public void close() throws ProductionException {
    }

    private void store(File databaseFile) throws IOException {
        // System.out.printf("%s: Storing %d production(s) to %s%n", this, productionsList.size(), databaseFile.getAbsolutePath());
        File parentFile = databaseFile.getParentFile();
        if (parentFile != null && !parentFile.exists()) {
            parentFile.mkdirs();
        }
        File bakFile = new File(parentFile, databaseFile.getName() + ".bak");
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
            String outputPath = decodeTSV(tokens[2]);
            String stagingPath = decodeTSV(tokens[3]);
            int[] offpt = new int[]{4};
            ProductionRequest productionRequest = decodeProductionRequestTSV(tokens, offpt);
            Object[] jobIDs = decodeJobIdsTSV(tokens, offpt);
            Date[] dates = decodeTimesTSV(tokens, offpt);
            ProcessStatus processStatus = decodeProductionStatusTSV(tokens, offpt);
            ProcessStatus stagingStatus = decodeProductionStatusTSV(tokens, offpt);
            WorkflowItem workflow = createWorkflow(jobIDs, dates, processStatus);
            boolean autoStaging = Boolean.parseBoolean(productionRequest.getString("autoStaging", "false"));
            Production production = new Production(id, name,
                                                   outputPath,
                                                   stagingPath,
                                                   autoStaging,
                                                   productionRequest,
                                                   workflow);
            production.setStagingStatus(stagingStatus);
            addProduction(production);
        }
    }

    void store(PrintWriter writer) {
        for (Production production : productionsList) {
            writer.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tEoR\n",
                          encodeTSV(production.getId()),
                          encodeTSV(production.getName()),
                          encodeTSV(production.getOutputPath()),
                          encodeTSV(production.getStagingPath()),
                          encodeProductionRequestTSV(production.getProductionRequest()),
                          encodeJobIdsTSV(production.getJobIds()),
                          encodeTimesTSV(production.getWorkflow()),
                          encodeProductionStatusTSV(production.getProcessingStatus()),
                          encodeProductionStatusTSV(production.getStagingStatus()));
        }
    }


    private Object[] decodeJobIdsTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        int numJobs = Integer.parseInt(tokens[off++]);
        Object[] jobIds = new Object[numJobs];
        for (int i = 0; i < numJobs; i++) {
            jobIds[i] = processingService.getJobIdFormat().parse(decodeTSV(tokens[off++]));
        }
        offpt[0] = off;
        return jobIds;
    }

    private Date[] decodeTimesTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        Date[] dates = new Date[3];
        for (int i = 0; i < dates.length; i++) {
            String text = decodeTSV(tokens[off++]);
            dates[i] = text != null ? new Date(Long.parseLong(text)) : null;
        }
        offpt[0] = off;
        return dates;
    }

    private String encodeTimesTSV(WorkflowItem workflowItem) {
        StringBuilder sb = new StringBuilder();
        sb.append(encodeTSV(encodeTime(workflowItem.getSubmitTime())));
        sb.append("\t");
        sb.append(encodeTSV(encodeTime(workflowItem.getStartTime())));
        sb.append("\t");
        sb.append(encodeTSV(encodeTime(workflowItem.getStopTime())));
        return sb.toString();
    }

    private String encodeTime(Date time) {
        return time != null ? Long.toString(time.getTime()) : null;
    }

    private String encodeJobIdsTSV(Object[] jobIds) {
        StringBuilder sb = new StringBuilder();
        sb.append(jobIds.length);
        for (Object jobId : jobIds) {
            sb.append("\t");
            sb.append(encodeTSV(processingService.getJobIdFormat().format(jobId)));
        }
        return sb.toString();
    }

    String encodeProductionRequestTSV(ProductionRequest productionRequest) {
        Map<String, String> productionParameters = productionRequest.getParameters();

        StringBuilder sb = new StringBuilder();
        sb.append(productionRequest.getProductionType());
        sb.append("\t");
        sb.append(productionRequest.getUserName());
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
        String userName = decodeTSV(tokens[off++]);
        int numParams = Integer.parseInt(tokens[off++]);
        Map<String, String> productionParameters = new HashMap<String, String>();
        for (int i = 0; i < numParams; i++) {
            String name = decodeTSV(tokens[off++]);
            String value = decodeTSV(tokens[off++]);
            productionParameters.put(name, value);
        }
        offpt[0] = off;
        return new ProductionRequest(productionType, userName, productionParameters);
    }

    static String encodeProductionStatusTSV(ProcessStatus processStatus) {
        StringBuilder sb = new StringBuilder();
        sb.append(processStatus.getState());
        sb.append("\t");
        sb.append(processStatus.getProgress());
        sb.append("\t");
        sb.append(encodeTSV(processStatus.getMessage()));
        return sb.toString();
    }

    static ProcessStatus decodeProductionStatusTSV(String[] tokens, int[] offpt) {
        int off = offpt[0];
        ProcessState processState = ProcessState.valueOf(tokens[off++]);
        float progress = Float.parseFloat(tokens[off++]);
        String message = encodeTSV(tokens[off++]);
        offpt[0] = off;
        return new ProcessStatus(processState, progress, message);
    }

    // TSV = tab separated characters
    private static String encodeTSV(String value) {
        if (value == null) {
            return "[[null]]";
        }
        return value.replace("\n", "\\n").replace("\t", "\\t");
    }

    // TSV = tab separated characters
    private static String decodeTSV(String tsv) {
        if (tsv.equals("[[null]]")) {
            return null;
        }
        return tsv.replace("\\n", "\n").replace("\\t", "\t");
    }

    /**
     * Creates the appropriate workflow for the given array of job identifiers.
     * The default implementation creates a proxy workflow that can neither be submitted, killed nor
     * updated.
     *
     * @param jobIds        Array of job identifiers
     * @param dates
     * @param processStatus
     * @return The workflow.
     */
    protected WorkflowItem createWorkflow(Object[] jobIds, Date[] dates, ProcessStatus processStatus) {
        return new ProxyWorkflow(processingService, jobIds, dates[0], dates[1], dates[2], processStatus);
    }
}
