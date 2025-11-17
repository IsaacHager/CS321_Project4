package cs321.search;

import cs321.common.ParseArgumentException;
import cs321.common.ParseArgumentUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulates the command-line arguments for SSHSearchDatabase.
 * It is responsible for parsing and validating the arguments.
 */
public class SSHSearchDatabaseArguments
{
    private static final Set<String> VALID_TYPES = new HashSet<>(Arrays.asList(
        "accepted-ip", "accepted-time", "invalid-ip", "invalid-time",
        "failed-ip", "failed-time", "reverseaddress-ip", "reverseaddress-time",
        "user-ip"
    ));

    private String databaseFile;
    private String logType;
    private int topFrequency;

    /**
     * Private constructor to enforce use of the factory method.
     */
    private SSHSearchDatabaseArguments() {
    }

    // --- Getters ---

    public String getDatabaseFile() {
        return databaseFile;
    }

    public String getLogType() {
        return logType;
    }

    public int getTopFrequency() {
        return topFrequency;
    }

    /**
     * Factory method to parse and create the arguments object.
     * @param args The command line arguments.
     * @return A fully initialized SSHSearchDatabaseArguments object.
     * @throws ParseArgumentException If any argument is missing or invalid.
     */
    public static SSHSearchDatabaseArguments parse(String[] args) throws ParseArgumentException {
        if (args.length == 0) {
            printUsageAndExit("Missing arguments.");
        }

        SSHSearchDatabaseArguments arguments = new SSHSearchDatabaseArguments();
        
        // Loop through arguments and parse
        for (String arg : args) {
            if (arg.startsWith("--database=")) {
                arguments.databaseFile = arg.substring("--database=".length());
            } else if (arg.startsWith("--type=")) {
                arguments.logType = arg.substring("--type=".length());
            } else if (arg.startsWith("--top-frequency=")) {
                String freqStr = arg.substring("--top-frequency=".length());
                arguments.topFrequency = ParseArgumentUtils.convertStringToInt(freqStr);
            } else {
                printUsageAndExit("Unknown argument: " + arg);
            }
        }
        
        // Validate arguments
        if (arguments.databaseFile == null) {
            printUsageAndExit("Missing required argument: --database=<filename>");
        }
        if (arguments.logType == null) {
            printUsageAndExit("Missing required argument: --type=<log-type>");
        }
        if (arguments.topFrequency <= 0) {
            printUsageAndExit("Missing or invalid required argument: --top-frequency=<N> (must be > 0)");
        }
        
        // Validate log type
        if (!VALID_TYPES.contains(arguments.logType)) {
            printUsageAndExit("Invalid log type specified: " + arguments.logType + 
                              ". Must be one of: " + VALID_TYPES);
        }

        return arguments;
    }

    /** * Print usage message and exit.
     * @param errorMessage the error message for proper usage
     */
    private static void printUsageAndExit(String errorMessage) throws ParseArgumentException
    {
        System.err.println("Error: " + errorMessage);
        System.err.println("Usage: java -jar SSHSearchDatabase.jar " +
                           "--database=<database-file> " +
                           "--type=<log-type> " +
                           "--top-frequency=<N> " +
                           "[--debug=<0|1>]");
        System.err.println("  <log-type> must be one of: " + VALID_TYPES);
        throw new ParseArgumentException(errorMessage);
    }
}