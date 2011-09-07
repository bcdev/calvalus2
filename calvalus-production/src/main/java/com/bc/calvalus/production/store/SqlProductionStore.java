package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * @return A new production store.
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
            }else {
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
        return values.toArray(new Production[0]);
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
        String id = resultSet.getString("id");
        String name = resultSet.getString("name");
        String productionType = resultSet.getString("production_type");
        String userName = resultSet.getString("user");
        String jobIdList = resultSet.getString("job_id_list");
        Date submitTime = resultSet.getDate("submit_time");
        Date startTime = resultSet.getDate("start_time");
        Date stopTime = resultSet.getDate("stop_time");
        String procState = resultSet.getString("processing_state");
        float procProgress = resultSet.getFloat("processing_progress");
        String procMessage = resultSet.getString("processing_message");
        String stagingState = resultSet.getString("staging_state");
        float stagingProgress = resultSet.getFloat("staging_progress");
        String stagingMessage = resultSet.getString("staging_message");
        String stagingPath = resultSet.getString("staging_path");
        boolean autoStaging = resultSet.getBoolean("auto_staging");
        // todo
        // String requestXml = resultSet.getString("request_xml");
        // ProductionRequest productionRequest = ProductionRequest.fromXml(requestXml);
        ProductionRequest productionRequest = new ProductionRequest(productionType, userName);
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
                                                                       "id, " +
                                                                       "name, " +
                                                                       "production_type, " +
                                                                       "user, " +
                                                                       "job_id_list, " +
                                                                       "submit_time, " +
                                                                       "start_time, " +
                                                                       "stop_time, " +
                                                                       "processing_state, " +
                                                                       "processing_progress, " +
                                                                       "processing_message, " +
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
                                                                       "?" +
                                                                       ")");
        }
        insertProductionStmt.clearParameters();
        insertProductionStmt.setString(1, production.getId());
        insertProductionStmt.setString(2, production.getName());
        insertProductionStmt.setString(3, production.getProductionRequest().getProductionType());
        insertProductionStmt.setString(4, production.getProductionRequest().getUserName());
        insertProductionStmt.setString(5, formatJobIds(processingService, production.getJobIds()));
        insertProductionStmt.setDate(6, toSqlDate(production.getWorkflow().getSubmitTime()));
        insertProductionStmt.setDate(7, toSqlDate(production.getWorkflow().getStartTime()));
        insertProductionStmt.setDate(8, toSqlDate(production.getWorkflow().getStopTime()));
        insertProductionStmt.setString(9, production.getProcessingStatus().getState().toString());
        insertProductionStmt.setFloat(10, production.getProcessingStatus().getProgress());
        insertProductionStmt.setString(11, production.getProcessingStatus().getMessage());
        insertProductionStmt.setString(12, production.getStagingStatus().getState().toString());
        insertProductionStmt.setFloat(13, production.getStagingStatus().getProgress());
        insertProductionStmt.setString(14, production.getStagingStatus().getMessage());
        insertProductionStmt.setString(15, production.getStagingPath());
        insertProductionStmt.setBoolean(16, production.isAutoStaging());
        insertProductionStmt.setString(17, production.getProductionRequest().toXml());
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
                                                                       " WHERE id=?");
        }
        updateProductionStmt.clearParameters();
        updateProductionStmt.setDate(1, toSqlDate(production.getWorkflow().getStartTime()));
        updateProductionStmt.setDate(2, toSqlDate(production.getWorkflow().getStopTime()));
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
            deleteProductionsStmt = connection.prepareStatement("DELETE FROM production WHERE id=?");
        }
        deleteProductionsStmt.clearParameters();
        deleteProductionsStmt.setString(1, productionId);
        deleteProductionsStmt.executeUpdate();
    }

    private void init() throws SQLException, IOException {
        InputStreamReader streamReader = new InputStreamReader(SqlProductionStore.class.getResourceAsStream("calvalus-store.sql"));
        try {
            SqlReader sqlReader = new SqlReader(streamReader);
            sqlReader.executeAll(connection);
            connection.commit();
        } finally {
            streamReader.close();
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

    private static Date toSqlDate(java.util.Date date) {
        return date != null ? new Date(date.getTime()) : null;
    }
}
