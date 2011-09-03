package com.bc.calvalus.production.store;

import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
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

    private final JobIdFormat jobIdFormat;
    private final Connection connection;

    public SqlProductionStore(JobIdFormat jobIdFormat, Connection connection) {
        this.jobIdFormat = jobIdFormat;
        this.connection = connection;
    }

    @Override
    public synchronized void addProduction(Production production) throws ProductionException {
        try {
            PreparedStatement statement = connection.prepareStatement("INSERT INTO Course VALUES (?, ?, ?, ?)");
            statement.clearParameters();
            statement.setString(1, production.getId());
            statement.setString(2, production.getName());
            statement.setString(3, production.getStagingPath());
            statement.setString(4, production.getProcessingStatus().toString());
            statement.setString(5, production.getStagingStatus().toString());
            statement.setString(6, Arrays.toString(production.getJobIds()));
            statement.setString(7, production.getProductionRequest().toXml());
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public synchronized void removeProduction(Production production) throws ProductionException {
    }

    @Override
    public synchronized Production[] getProductions() throws ProductionException {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM Production");
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
            ResultSet resultSet = statement.executeQuery("SELECT * FROM Production WHERE ID='" + productionId + "'");
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
        String id = resultSet.getString(1);
        String name = resultSet.getString(2);
        String stagingPath = resultSet.getString(3);
        boolean autoStaging = resultSet.getBoolean(4);
        String pdrXml = resultSet.getString(5);
        ProductionRequest productionRequest = ProductionRequest.fromXml(pdrXml);
        return new Production(id, name, stagingPath, autoStaging, productionRequest, null);
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
        return new SqlProductionStore(JobIdFormat.STRING, connection);
    }

    public static SqlProductionStore open(String url) throws SQLException, IOException {
        return new SqlProductionStore(JobIdFormat.STRING, getConnection(url));
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


}
