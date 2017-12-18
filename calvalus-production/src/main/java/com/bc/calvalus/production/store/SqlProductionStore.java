package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * A persistent store for productions that uses an SQL database.
 * <p/>
 * This store's {@link #persist()} method will update the database with production status information
 * retrieved from the processing system (e.g. Hadoop). It does NOT update cached production objects
 * with status information retrieved from the database. With other words, the only source of status
 * information is the processing system. Thus this service assumes, that no 2nd store exists, that
 * updates the database with production status information.
 *
 * @author Norman
 */
public class SqlProductionStore implements ProductionStore {

    private final ProcessingService processingService;
    private final Connection connection;

    private final Map<String, Production> cachedProductions;
    private final Set<String> addedProductionIds;
    private final Set<String> removedProductionIds;

    private PreparedStatement deleteProductionsStmt;
    private PreparedStatement insertProductionStmt;
    private PreparedStatement updateProductionStmt;

    /**
     * Creates a new production store.
     *
     * @param processingService The processing service.
     * @param url               The database URL.
     * @param user              The database user's name.
     * @param password          The database user' password.
     * @param init              If {@code true}, the database initialisation SQL script {@code calvalus-init.sql} will be run.
     *                          If {@code false}, {@link #update()} will be called immediately after the store has been created.
     *
     * @return A new production store.
     *
     * @throws ProductionException If an error occurs.
     */

    public static SqlProductionStore create(ProcessingService processingService,
                                            String driver,
                                            String url,
                                            String user,
                                            String password,
                                            boolean init) throws ProductionException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ProductionException("Failed to load database driver " + driver);
        }

        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            SqlProductionStore store = new SqlProductionStore(processingService, connection);
            if (init) {
                store.init();
            } else {
                store.update();
            }
            return store;
        } catch (SQLException e) {
            throw new ProductionException("Failed to create production store: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new ProductionException("Failed to create production store: " + e.getMessage(), e);
        }
    }

    private SqlProductionStore(ProcessingService processingService, Connection connection) {
        this.processingService = processingService;
        this.connection = connection;
        this.cachedProductions = new HashMap<String, Production>(73);
        this.addedProductionIds = new HashSet<String>();
        this.removedProductionIds = new HashSet<String>();
    }

    @Override
    public synchronized void addProduction(Production production) {
        cachedProductions.put(production.getId(), production);
        addedProductionIds.add(production.getId());
        removedProductionIds.remove(production.getId());
    }

    @Override
    public synchronized void removeProduction(String productionId) {
        cachedProductions.remove(productionId);
        addedProductionIds.remove(productionId);
        removedProductionIds.add(productionId);
    }

    @Override
    public synchronized Production[] getProductions() {
        Collection<Production> values = cachedProductions.values();
        return values.toArray(new Production[values.size()]);
    }

    @Override
    public synchronized Production getProduction(String productionId) {
        return cachedProductions.get(productionId);
    }

    @Override
    public synchronized void update() throws ProductionException {
        try {
            List<Production> productionList = selectProductions();
            for (Production production : productionList) {
                Production cachedProduction = cachedProductions.get(production.getId());
                if (cachedProduction != null) {
                    // todo
                    // cachedProduction.setSubmitTime(...)
                    // cachedProduction.setStartTime(...)
                    // cachedProduction.setStopTime(...)
                    cachedProduction.setProcessingStatus(production.getProcessingStatus());
                    cachedProduction.setStagingStatus(production.getStagingStatus());
                } else {
                    cachedProductions.put(production.getId(), production);
                }
            }
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public synchronized void persist() throws ProductionException {
        try {
            for (String productionId : removedProductionIds) {
                deleteProduction(productionId);
            }

            for (String productionId : addedProductionIds) {
                insertProduction(cachedProductions.get(productionId));
            }

            for (String productionId : cachedProductions.keySet()) {
                if (!addedProductionIds.contains(productionId) &&
                    !removedProductionIds.contains(productionId)) {
                    updateProductionStatus(cachedProductions.get(productionId));
                }
            }

            connection.commit();

            addedProductionIds.clear();
            removedProductionIds.clear();

        } catch (SQLException e) {
            throw new ProductionException("Failed to persist production store: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws ProductionException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new ProductionException("Failed to close production store: " + e.getMessage(), e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    private List<Production> selectProductions() throws SQLException {
        PreparedStatement selectProductions = connection.prepareStatement("SELECT * FROM production");
        ResultSet resultSet = selectProductions.executeQuery();
        ArrayList<Production> productions = new ArrayList<Production>(100 + cachedProductions.size());
        while (resultSet.next()) {
            Production production = getNextProduction(resultSet);
            productions.add(production);
        }
        return productions;
    }

    private Production getNextProduction(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("production_id");
        String name = resultSet.getString("production_name");
        String productionType = resultSet.getString("production_type");
        String userName = resultSet.getString("production_user");
        String jobIdList = resultSet.getString("job_id_list");
        Timestamp submitTime = resultSet.getTimestamp("submit_time");
        Timestamp startTime = resultSet.getTimestamp("start_time");
        Timestamp stopTime = resultSet.getTimestamp("stop_time");
        String procState = resultSet.getString("processing_state");
        float procProgress = resultSet.getFloat("processing_progress");
        String procMessage = resultSet.getString("processing_message");
        String outputPath = resultSet.getString("output_path");
        String[] intermediatePathes = new String[0];
        if (outputPath != null) {
            if (outputPath.contains(";")) {
                String[] elems = outputPath.split(";");
                outputPath = decodeNull(elems[0]);
                intermediatePathes = new String[elems.length - 1];
                for (int i = 1; i < elems.length; i++) {
                    intermediatePathes[i - 1] = decodeNull(elems[i]);
                }
            } else {
                outputPath = decodeNull(outputPath);
            }
        }
        String stagingState = resultSet.getString("staging_state");
        float stagingProgress = resultSet.getFloat("staging_progress");
        String stagingMessage = resultSet.getString("staging_message");
        String stagingPath = resultSet.getString("staging_path");
        boolean autoStaging = resultSet.getBoolean("auto_staging");

        String requestXml = resultSet.getString("request_xml");
        ProductionRequest productionRequest = new ProductionRequest(productionType, userName);
        if (requestXml != null) {
            try {
                productionRequest = ProductionRequest.fromXml(requestXml);
            } catch (Exception e) {
                CalvalusLogger.getLogger().log(Level.WARNING, "Could not retrieve production request from database.",
                                               e);
            }
        }
        Object[] jobIds = parseJobIds(processingService, jobIdList);
        ProcessStatus processStatus = new ProcessStatus(ProcessState.valueOf(procState),
                                                        procProgress,
                                                        procMessage);
        ProcessStatus stagingStatus = new ProcessStatus(ProcessState.valueOf(stagingState),
                                                        stagingProgress,
                                                        stagingMessage);
        ProxyWorkflow workflow = new ProxyWorkflow(processingService,
                                                   jobIds,
                                                   submitTime,
                                                   startTime,
                                                   stopTime,
                                                   processStatus);
        Production production = new Production(id, name,
                                               outputPath,
                                               intermediatePathes,
                                               stagingPath,
                                               autoStaging,
                                               productionRequest,
                                               workflow);
        production.setStagingStatus(stagingStatus);
        return production;
    }

    private void insertProduction(Production production) throws SQLException {
        if (insertProductionStmt == null) {
            insertProductionStmt = connection.prepareStatement("INSERT INTO production " +
                                                               "(" +
                                                               "production_id, " +
                                                               "production_name, " +
                                                               "production_type, " +
                                                               "production_user, " +
                                                               "job_id_list, " +
                                                               "submit_time, " +
                                                               "start_time, " +
                                                               "stop_time, " +
                                                               "processing_state, " +
                                                               "processing_progress, " +
                                                               "processing_message, " +
                                                               "output_path, " +
                                                               "staging_state, " +
                                                               "staging_progress, " +
                                                               "staging_message, " +
                                                               "staging_path, " +
                                                               "auto_staging, " +
                                                               "request_xml" +
                                                               ") " +
                                                               " VALUES " +
                                                               "(" +
                                                               "?, ?, ?, ?, " +
                                                               "?, ?, ?, ?, " +
                                                               "?, ?, ?, ?, " +
                                                               "?, ?, ?, ?, " +
                                                               "?, ?" +
                                                               ")");
        }
        insertProductionStmt.clearParameters();
        insertProductionStmt.setString(1, production.getId());
        insertProductionStmt.setString(2, production.getName());
        insertProductionStmt.setString(3, production.getProductionRequest().getProductionType());
        insertProductionStmt.setString(4, production.getProductionRequest().getUserName());
        insertProductionStmt.setString(5, formatJobIds(processingService, production.getJobIds()));
        insertProductionStmt.setTimestamp(6, toSqlTimestamp(production.getWorkflow().getSubmitTime()));
        insertProductionStmt.setTimestamp(7, toSqlTimestamp(production.getWorkflow().getStartTime()));
        insertProductionStmt.setTimestamp(8, toSqlTimestamp(production.getWorkflow().getStopTime()));
        insertProductionStmt.setString(9, production.getProcessingStatus().getState().toString());
        insertProductionStmt.setFloat(10, production.getProcessingStatus().getProgress());
        insertProductionStmt.setString(11, production.getProcessingStatus().getMessage());
        insertProductionStmt.setString(12, formatOutputPathes(production.getOutputPath(), production.getIntermediateDataPath()));
        insertProductionStmt.setString(13, production.getStagingStatus().getState().toString());
        insertProductionStmt.setFloat(14, production.getStagingStatus().getProgress());
        insertProductionStmt.setString(15, production.getStagingStatus().getMessage());
        insertProductionStmt.setString(16, production.getStagingPath());
        insertProductionStmt.setBoolean(17, production.isAutoStaging());
        insertProductionStmt.setString(18, production.getProductionRequest().toXml());
        insertProductionStmt.executeUpdate();
    }

    private void updateProductionStatus(Production production) throws SQLException {
        if (updateProductionStmt == null) {
            updateProductionStmt = connection.prepareStatement("UPDATE production SET " +
                                                               "start_time=?, " +
                                                               "stop_time=?, " +
                                                               "processing_state=?, " +
                                                               "processing_progress=?, " +
                                                               "processing_message=?, " +
                                                               "staging_state=?, " +
                                                               "staging_progress=?, " +
                                                               "staging_message=? " +
                                                               " WHERE production_id=?");
        }
        updateProductionStmt.clearParameters();
        updateProductionStmt.setTimestamp(1, toSqlTimestamp(production.getWorkflow().getStartTime()));
        updateProductionStmt.setTimestamp(2, toSqlTimestamp(production.getWorkflow().getStopTime()));
        updateProductionStmt.setString(3, production.getProcessingStatus().getState().toString());
        updateProductionStmt.setFloat(4, production.getProcessingStatus().getProgress());
        updateProductionStmt.setString(5, production.getProcessingStatus().getMessage());
        updateProductionStmt.setString(6, production.getStagingStatus().getState().toString());
        updateProductionStmt.setFloat(7, production.getStagingStatus().getProgress());
        updateProductionStmt.setString(8, production.getStagingStatus().getMessage());
        updateProductionStmt.setString(9, production.getId());
        updateProductionStmt.executeUpdate();
    }

    private void deleteProduction(String productionId) throws SQLException {
        if (deleteProductionsStmt == null) {
            deleteProductionsStmt = connection.prepareStatement("DELETE FROM production WHERE production_id=?");
        }
        deleteProductionsStmt.clearParameters();
        deleteProductionsStmt.setString(1, productionId);
        deleteProductionsStmt.executeUpdate();
    }

    private void init() throws SQLException, IOException {
        Reader reader = new BufferedReader(new InputStreamReader(
                SqlProductionStore.class.getResourceAsStream("calvalus-store.sql")));
        try {
            SqlReader sqlReader = new SqlReader(reader);
            sqlReader.executeAll(connection);
            connection.commit();
        } finally {
            reader.close();
        }
    }

    private static Object[] parseJobIds(ProcessingService processingService, String jobIdList) {
        String[] jobIdsStrings = jobIdList.split(",");
        Object[] jobIds = new Object[jobIdsStrings.length];
        for (int i = 0; i < jobIds.length; i++) {
            jobIds[i] = processingService.getJobIdFormat().parse(jobIdsStrings[i]);
        }
        return jobIds;
    }

    private static String formatJobIds(ProcessingService processingService, Object[] jobIds) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < jobIds.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(processingService.getJobIdFormat().format(jobIds[i]));
        }
        return sb.toString();
    }

    private static Timestamp toSqlTimestamp(Date date) {
        return date != null ? new Timestamp(date.getTime()) : null;
    }

    private static String formatOutputPathes(String outputPath, String[] intermediateDataPath) {
        StringBuilder sb = new StringBuilder(encodeNull(outputPath));
        for (String path : intermediateDataPath) {
            sb.append(";").append(encodeNull(path));
        }
        return sb.toString();
    }

    private static String encodeNull(String s) {
        return s == null ? "[[null]]" : s;
    }

    private static String decodeNull(String s) {
        return "[[null]]".equals(s) ? null : s;
    }
}
