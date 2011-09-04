package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * An SQL-based database for productions.
 *
 * @author Norman
 */
public class SqlProductionStore implements ProductionStore {

    private final ProcessingService processingService;
    private final Connection connection;

    public SqlProductionStore(ProcessingService processingService, Connection connection) {
        this.processingService = processingService;
        this.connection = connection;
    }

    @Override
    public synchronized void addProduction(Production production) throws ProductionException {
        try {
            PreparedStatement statement = connection.prepareStatement("INSERT INTO production " +
                                                                              "(" +
                                                                              "id, " +
                                                                              "name, " +
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
                                                                              "VALUES " +
                                                                              "(" +
                                                                              "?, ?, ?, ?," +
                                                                              "?, ?, ?, ?," +
                                                                              "?, ?, ?, ?," +
                                                                              "?, ?, ?, ?" +
                                                                              ")");
            statement.clearParameters();
            statement.setString(1, production.getId());
            statement.setString(2, production.getName());
            statement.setString(3, production.getProductionRequest().getUserName());
            statement.setString(4, Arrays.toString(production.getJobIds()));
            statement.setDate(5, new java.sql.Date(production.getWorkflow().getSubmitTime().getTime()));
            statement.setDate(6, new java.sql.Date(production.getWorkflow().getStartTime().getTime()));
            statement.setDate(7, new java.sql.Date(production.getWorkflow().getStopTime().getTime()));
            statement.setString(8, production.getProcessingStatus().getState().toString());
            statement.setFloat(9, production.getProcessingStatus().getProgress());
            statement.setString(10, production.getProcessingStatus().getMessage());
            statement.setString(11, production.getStagingStatus().getState().toString());
            statement.setFloat(12, production.getStagingStatus().getProgress());
            statement.setString(13, production.getStagingStatus().getMessage());
            statement.setString(14, production.getStagingPath());
            statement.setBoolean(15, production.isAutoStaging());
            statement.setString(16, production.getProductionRequest().toXml());
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public synchronized void removeProduction(Production production) throws ProductionException {
        // todo
    }

    @Override
    public synchronized Production[] getProductions() throws ProductionException {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM production");
            ArrayList<Production> productions = new ArrayList<Production>(512);
            while (resultSet.next()) {
                productions.add(createProduction(resultSet));
            }
            return productions.toArray(new Production[productions.size()]);
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public synchronized Production getProduction(String productionId) throws ProductionException {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM production WHERE id='" + productionId + "'");
            if (resultSet.next()) {
                return createProduction(resultSet);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public synchronized void load() throws ProductionException {
    }

    @Override
    public synchronized void store() throws ProductionException {
    }

    @Override
    public void close() throws ProductionException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    private Production createProduction(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String name = resultSet.getString("name");
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
        String requestXml = resultSet.getString("request_xml");

        ProductionRequest productionRequest = ProductionRequest.fromXml(requestXml);
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

    public void commit() throws SQLException {
        connection.commit();
    }

    private static Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:" + url, "SA", "");
    }

    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {
            System.err.println("Error: failed to load HSQLDB JDBC driver.");
            e.printStackTrace(System.err);
        }
    }

    public static SqlProductionStore create(String url) throws SQLException, IOException {
        Connection connection = getConnection(url);
        InputStream stream = SqlProductionStore.class.getResourceAsStream("calvalus-store.sql");
        InputStreamReader streamReader = new InputStreamReader(stream);
        SqlReader sqlReader = new SqlReader(streamReader);
        String sql;
        Statement statement = connection.createStatement();
        while ((sql = sqlReader.readSql()) != null) {
            statement.executeUpdate(sql);
        }
        statement.close();
        connection.commit();
        return new SqlProductionStore(null, connection);
    }

    public static SqlProductionStore open(String url) throws SQLException, IOException {
        return new SqlProductionStore(null, getConnection(url));
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: " + SqlProductionStore.class.getSimpleName().toLowerCase() + " [status] | [new] | [test]");
            return;
        }
        String command = args[0];
        try {
            File databaseDir = new File("./database").getAbsoluteFile();
            if (command.equalsIgnoreCase("new")) {
                newDb(databaseDir);
            } else if (command.equalsIgnoreCase("test")) {
                testDb(databaseDir);
            } else {
                status(databaseDir);
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }

    }

    private static void newDb(File databaseDir) throws Exception {
        if (databaseDir.exists()) {
            databaseDir.mkdirs();
        }
        SqlProductionStore gsmdb = SqlProductionStore.create(String.format("file:%s/calvalus-store;shutdown=true",
                                                                           databaseDir));
        gsmdb.commit();
        gsmdb.close();
    }

    private static void testDb(File databaseDir) throws Exception {
        if (databaseDir.exists()) {
            databaseDir.mkdirs();
        }
        SqlProductionStore store = SqlProductionStore.create(String.format("file:%s/calvalus-store;shutdown=true", databaseDir));
        for (int i = 1; i <= 100; i++) {
            store.addProduction(new Production("P" + i,
                                               "N" + i,
                                               "/out/" + i,
                                               true,
                                               new ProductionRequest("X", "eva", "a", "1", "b", "2"),
                                               null));
        }
        store.commit();
        store.close();
    }

    private static void status(File databaseDir) throws Exception {
        SqlProductionStore store = SqlProductionStore.open(String.format("file:%s/calvalus-store", databaseDir));

        Production[] productions = store.getProductions();
        System.out.println(productions.length + " productions(s)");
        for (Production production : productions) {
            System.out.println("  " + production);
        }
        store.close();
    }

    private static Object[] parseJobIds(ProcessingService processingService, String jobIdList) {
        String[] jobIdsStrings = jobIdList.split(",");
        Object[] jobIds = new Object[jobIdsStrings.length];
        for (int i = 0; i < jobIds.length; i++) {
            jobIds[i] = processingService.getJobIdFormat().parse(jobIdsStrings[i]);
        }
        return jobIds;
    }

}
