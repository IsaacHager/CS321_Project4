package cs321.search;

import cs321.btree.BTree;
import cs321.common.ParseArgumentException;
import cs321.common.ParseArgumentUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

/**
 * The driver class for searching a Database of a B-Tree.
 * It connects to a SQLite database and retrieves the top N key frequencies
 * for a specified log type/table.
 */
public class SSHSearchDatabase
{
    
    public static void main(String[] args) throws Exception
    {
        System.out.println("Hello world from cs321.search.SSHSearchDatabase.main");

        // 1. Parse Arguments
        SSHSearchDatabaseArguments arguments = null;
        try {
            arguments = SSHSearchDatabaseArguments.parse(args);
        } catch (ParseArgumentException e) {
            // The parse method handles printing usage and exiting via exception
            // We just need to ensure the main method catches it.
            System.err.println("Error during argument parsing: " + e.getMessage());
            System.exit(1);
        }

        // 2. Execute Database Search
        try {
            searchDatabase(arguments);
        } catch (SQLException e) {
            System.err.println("Database error occurred: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Connects to the SQLite database and executes a query to find the top N
     * most frequent keys for the specified table (log type).
     *
     * @param arguments The parsed command-line arguments.
     * @throws SQLException If a database access error occurs.
     */
    private static void searchDatabase(SSHSearchDatabaseArguments arguments) throws SQLException
    {
        // SQLite JDBC driver is assumed to be available via build.gradle
        String dbUrl = "jdbc:sqlite:" + arguments.getDatabaseFile();
        String tableName = arguments.getLogType();
        int topN = arguments.getTopFrequency();

        // SQL query: Select key and count, order by count (descending) and limit to topN
        // We assume the table was created by BTree.dumpToDatabase with columns (key TEXT, count INTEGER)
        String sqlQuery = String.format(
            "SELECT key, count FROM \"%s\" ORDER BY count DESC LIMIT %d",
            tableName, topN
        );
        
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            // Establish connection
            connection = DriverManager.getConnection(dbUrl);
            
            // Create and execute statement
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlQuery);

            // Print results to standard output
            while (resultSet.next()) {
                String key = resultSet.getString("key");
                long count = resultSet.getLong("count");
                // The expected output format is typically: key <tab> count
                System.out.printf("%s\t%d%n", key, count);
            }
        } finally {
            // Close resources in reverse order to prevent resource leaks
            if (resultSet != null) {
                try { resultSet.close(); } catch (SQLException e) { /* Ignore */ }
            }
            if (statement != null) {
                try { statement.close(); } catch (SQLException e) { /* Ignore */ }
            }
            if (connection != null) {
                try { connection.close(); } catch (SQLException e) { /* Ignore */ }
            }
        }
    }
}