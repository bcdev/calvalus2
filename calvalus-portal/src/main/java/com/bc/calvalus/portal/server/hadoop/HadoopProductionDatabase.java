package com.bc.calvalus.portal.server.hadoop;

import org.apache.hadoop.mapreduce.JobID;

import javax.servlet.ServletException;
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

    public HadoopProductionDatabase() throws ServletException, IOException {
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

    public synchronized void load(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\t");
            String id = tokens[0];
            String name = tokens[1];
            String outputPath = tokens[2];
            String jobId = tokens[3];
            addProduction(new HadoopProduction(id, name, outputPath, JobID.forName(jobId)));
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

    public synchronized void store(PrintWriter writer) {
        for (HadoopProduction production : productionsList) {
            writer.printf("%s\t%s\t%s\t%s\n",
                          production.getId(),
                          production.getName(),
                          production.getOutputPath(),
                          production.getJobId());
        }
    }
}
