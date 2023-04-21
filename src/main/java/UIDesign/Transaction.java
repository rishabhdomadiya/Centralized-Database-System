package UIDesign;

import Query.Parser.InvalidSyntaxException;
import Query.Parser.QueryParser;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Transaction {
    static Pattern DROP_TABLE = Pattern.compile("Drop table (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern SHOW_TABLE = Pattern.compile("Show table (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern COMMIT = Pattern.compile("commit transaction (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern ROLLBACK = Pattern.compile("rollback transaction (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern USE_DATABASE = Pattern.compile("Use (.*);", Pattern.CASE_INSENSITIVE);
    static HashMap<String, Set<String>> lockTable;

    Pattern INSERT_TABLE = Pattern.compile("Insert into (.*) \\((.*(,?)( ?)).*\\) values \\((.*(,?)( ?)).*\\);", Pattern.CASE_INSENSITIVE);
    QueryParser queryParser;
    String transactionName;
    FileWriter eventLogs;
    FileWriter generalLogs;
    FileWriter queryLogs;


    public Transaction(String transactionName, FileWriter eventLogs, FileWriter generalLogs, FileWriter queryLogs) {
        this.transactionName = transactionName;
        this.eventLogs = eventLogs;
        this.generalLogs = generalLogs;
        this.queryLogs = queryLogs;
    }

    private static boolean checkTableLock(String txnName, HashMap<String, Set<String>> lockTable, String username) {

        Set<String> temp = new HashSet<>();
        for (String key : lockTable.keySet()) {
            temp.addAll(lockTable.get(key));
        }

        return temp.contains(username + "_" + txnName);
    }

    public void performTransaction(String username) throws InvalidSyntaxException, IOException {

        List<String> queryList = new ArrayList<>();
        String tableName;
        String query;
        lockTable = new HashMap<>();
        Scanner reader = new Scanner(System.in);
        queryParser = new QueryParser(eventLogs, generalLogs, queryLogs);
        Set<String> lockedEntity = new HashSet<>();
        lockTable.put(username + "_" + transactionName, lockedEntity);

        while (reader.hasNext()) {
            query = reader.nextLine();

            Matcher commitQueryMatcher = COMMIT.matcher(query);
            Matcher rollbackQueryMatcher = ROLLBACK.matcher(query);
            Matcher useDatabaseMatcher = USE_DATABASE.matcher(query);
            Matcher dropQueryMatcher = DROP_TABLE.matcher(query);
            Matcher insertQueryMatcher = INSERT_TABLE.matcher(query);
            Matcher showQueryMatcher = SHOW_TABLE.matcher(query);
            // Logs related
            Date date = new Date();
            // getTime() returns current time in milliseconds
            long time = date.getTime();
            // Passed the milliseconds to constructor of Timestamp class
            Timestamp ts = new Timestamp(time);

            if (commitQueryMatcher.find()) {
                for (String q : queryList) {
                    queryParser.parseQuery(q, username);
                }
                queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(username).append(" ] [Query Operation: Commit Transaction] [Query: ").append(query).append("] [Query Type: Valid]\n");
                eventLogs.append("[User: ").append(username).append("] [Query: ").append(query).append("]\n");
                lockTable.remove(username + "_" + transactionName);
                return;
            }

            if (rollbackQueryMatcher.find()) {
                queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(username).append(" ] [Query Operation: Rollback Transaction] [Query: ").append(query).append("] [Query Type: Valid]\n");
                eventLogs.append("[User: ").append(username).append("] [Query: ").append(query).append("]\n");
                return;
            }

            if (useDatabaseMatcher.find()) {
                queryList.add(query);
            }

            if (dropQueryMatcher.find()) {
                queryList.add(query);
                tableName = dropQueryMatcher.group(1);
                if (!checkTableLock(tableName, lockTable, username)) {
                    Set<String> lockedTables = lockTable.get(username + "_" + transactionName);
                    lockedTables.add(tableName);
                    lockTable.put(username + "_" + transactionName, lockedTables);


                } else {
                    System.out.println("The table " + tableName + " is already locked");
                    break;
                }
            }

            if (insertQueryMatcher.find()) {
                queryList.add(query);
                tableName = insertQueryMatcher.group(1);
                if (!checkTableLock(tableName, lockTable, username)) {
                    Set<String> lockedTables = lockTable.get(username + "_" + transactionName);
                    lockedTables.add(tableName);
                    lockTable.put(username + "_" + transactionName, lockedTables);
                } else {
                    System.out.println("The table " + tableName + " is already locked");
                    break;
                }
            }

            if (showQueryMatcher.find()) {
                queryList.add(query);
                tableName = showQueryMatcher.group(1);
                if (!checkTableLock(tableName, lockTable, username)) {
                    Set<String> lockedTables = lockTable.get(username + "_" + transactionName);
                    lockedTables.add(tableName);
                    lockTable.put(username + "_" + transactionName, lockedTables);
                } else {
                    System.out.println("The table " + tableName + " is already locked");
                    break;
                }
            }
        }
        System.out.println(lockTable.toString());
    }
}
