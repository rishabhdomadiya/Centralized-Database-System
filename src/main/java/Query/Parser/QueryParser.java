package Query.Parser;

import UIDesign.Transaction;
import Query.Processor.QueryProcessor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser {
    Pattern SHOW_DATABASE = Pattern.compile("show databases;", Pattern.CASE_INSENSITIVE);
    Pattern CREATE_DATABASE = Pattern.compile("Create database (.*);", Pattern.CASE_INSENSITIVE);
    Pattern USE_DATABASE = Pattern.compile("Use (.*);", Pattern.CASE_INSENSITIVE);
    Pattern DROP_DATABASE = Pattern.compile("Drop database (.*);", Pattern.CASE_INSENSITIVE);
    Pattern CREATE_TABLE = Pattern.compile("Create table (.*) \\(((.*) (.*)(,?)( ?))*\\);", Pattern.CASE_INSENSITIVE);
    Pattern DROP_TABLE = Pattern.compile("Drop table (.*);", Pattern.CASE_INSENSITIVE);
    Pattern SHOW_TABLES = Pattern.compile("show tables;", Pattern.CASE_INSENSITIVE);
    Pattern INSERT_TABLE = Pattern.compile("Insert into (.*) \\((.*(,?)( ?)).*\\) values \\((.*(,?)( ?)).*\\);", Pattern.CASE_INSENSITIVE);
    Pattern SHOW_TABLE = Pattern.compile("Show table (.*);", Pattern.CASE_INSENSITIVE);
    Pattern SELECT_ALL_TABLE = Pattern.compile("Select \\* from (.*);", Pattern.CASE_INSENSITIVE);
    Pattern SELECT_CLAUSE = Pattern.compile("Select \\* from (.*) where (.*)=(.*);", Pattern.CASE_INSENSITIVE);
    Pattern DELETE_CLAUSE = Pattern.compile("Delete from (.*) where (.*)=(.*);", Pattern.CASE_INSENSITIVE);
    Pattern UPDATE_CLAUSE = Pattern.compile("Update (.*) Set (.*)=(.*) where (.*)=(.*);", Pattern.CASE_INSENSITIVE);
    static Pattern BEGIN_TRANSACTION = Pattern.compile("Begin transaction (.*);", Pattern.CASE_INSENSITIVE);


    FileWriter eventLogs;
    FileWriter generalLogs;
    FileWriter queryLogs;
    QueryProcessor qp;

    public QueryParser(FileWriter eventLogs, FileWriter generalLogs, FileWriter queryLogs) {
        this.eventLogs = eventLogs;
        this.generalLogs = generalLogs;
        this.queryLogs = queryLogs;
        this.qp = new QueryProcessor();
    }


    public void parseQuery(String query, String Username) throws InvalidSyntaxException, IOException {
        boolean invalidQuery = true;
        long queryStartTime, queryEndTime, elapsedTime;

        // Logs related
        Date date = new Date();
        // getTime() returns current time in milliseconds
        long time = date.getTime();
        // Passed the milliseconds to constructor of Timestamp class
        Timestamp ts = new Timestamp(time);

        Matcher matcher = BEGIN_TRANSACTION.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String transactionName = matcher.group(1);
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append(" ] Transaction Name: ").append(transactionName).append("] [Query Operation: begin transaction] [Query: ").append(query).append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            Transaction transaction = new Transaction(transactionName, eventLogs, generalLogs, queryLogs);
            transaction.performTransaction(Username);
            return;
        }

        queryStartTime = System.nanoTime();

        matcher = SHOW_DATABASE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            qp.showDatabases();
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append(" ] [Query Operation: show database] [Query: ").append(query).append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
        }

        matcher = SHOW_TABLES.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            qp.showTables();
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append(" ] [Query Operation: show tables] [Query: ").append(query).append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
        }

        matcher = CREATE_DATABASE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String dbName = matcher.group(1);
            if (matcher.group(1).contains(" ")) {
                queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                                "[Database: ").append(dbName).append(" ] [Query Operation: create database] [Query: ").append(query)
                        .append("] [Query Type: Invalid]\n");
                throw new InvalidSyntaxException("Cannot create 2 databases at a time!");
            }
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                            "[Database: ").append(dbName).append(" ] [Query Operation: create database] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.createDatabase(dbName);
        }

        matcher = USE_DATABASE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String dbName = matcher.group(1);
            if (matcher.group(1).contains(" ")) {
                queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                                "[Database: ").append(dbName).append(" ] [Query Operation: use] [Query: ").append(query)
                        .append("] [Query Type: Valid]\n");
                throw new InvalidSyntaxException("Cannot use 2 databases at a time!");
            }
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                            "[Database: ").append(dbName).append(" ] [Query Operation: use ] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.useDatabase(dbName);
        }

        matcher = DROP_DATABASE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String dbName = matcher.group(1);
            if (matcher.group(1).contains(" ")) {
                queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                                "[Database: ").append(dbName).append(" ] [Query Operation: drop database] [Query: ").append(query)
                        .append("] [Query Type: Valid]\n");
                throw new InvalidSyntaxException("Cannot drop 2 databases at a time!");
            }
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                            "[Database: ").append(dbName).append(" ] [Query Operation: drop database] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.dropDatabase(dbName);
        }

        matcher = CREATE_TABLE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String tableName = matcher.group(1);
            String[] tableData = matcher.group(2).split(",");
            HashMap<String, String> tableColumnData = new HashMap<>();
            for (String data : tableData) {
                String[] column = data.trim().split(" ");
                if (column.length == 2) {
                    tableColumnData.put(column[0], column[1]);
                } else if (column[0].equalsIgnoreCase("PRIMARY")) {
                    tableColumnData.put("primary_key", column[2]);
                } else if (column[0].equalsIgnoreCase("FOREIGN")) {
                    tableColumnData.put("foreign_key", column[3] + "." + column[2]);
                } else {
                    throw new InvalidSyntaxException("Invalid syntax for column name or datatype!");
                }

            }
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.createTable(tableName, tableColumnData, queryLogs, ts, Username, query);
        }

        matcher = DROP_TABLE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String tableName = matcher.group(1);
            if (matcher.group(1).contains(" ")) {
                throw new InvalidSyntaxException("Cannot drop 2 tables at a time!");
            }

            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.dropTable(tableName, queryLogs, ts, Username, query);
        }

        matcher = INSERT_TABLE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String tableName = matcher.group(1);
            String[] colNames = matcher.group(2).split(",");
            String[] rowData = matcher.group(5).split(",");
            HashMap<String, String> tableRowData = new HashMap<>();
            if (colNames.length == rowData.length) {
                for (int i = 0; i < colNames.length; i++) {
                    tableRowData.put(colNames[i].trim(), rowData[i].trim());
                }
            } else {
                throw new InvalidSyntaxException("Invalid syntax for column name or datatype!");
            }
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.writeTableData(tableName, tableRowData, queryLogs, ts, Username, query);
        }

        matcher = SHOW_TABLE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] [Query Operation: show] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.showTable(matcher.group(1));
        }

        matcher = SELECT_ALL_TABLE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] [Query Operation: show] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.showTable(matcher.group(1));
        }

        matcher = SELECT_CLAUSE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String tableName = matcher.group(1);
            String attrName = matcher.group(2);
            String attrVal = matcher.group(3);
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.showSelectData(tableName, attrName, attrVal);
        }

        matcher = DELETE_CLAUSE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String tableName = matcher.group(1);
            String attrName = matcher.group(2);
            String attrVal = matcher.group(3);
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.deleteRow(tableName, attrName, attrVal, queryLogs, ts, Username, query);
        }

        matcher = UPDATE_CLAUSE.matcher(query);
        if (matcher.find()) {
            invalidQuery = false;
            String tableName = matcher.group(1);
            String attrName = matcher.group(2);
            String newAttrVal = matcher.group(3);
            String condName = matcher.group(4);
            String condAttrVal = matcher.group(5);
            eventLogs.append("[User: ").append(Username).append("] [Query: ").append(query).append("]\n");
            qp.updateRow(tableName, attrName, newAttrVal, condName, condAttrVal, queryLogs, ts, Username, query);
        }
        queryEndTime = System.nanoTime();
        elapsedTime = queryEndTime - queryStartTime;
        String databaseName = qp.currentDatabase;
        if (databaseName != null) {
            File file = new File("src/main/resources/databases/" + databaseName);
            int totalTables = file.listFiles().length - 1;
            generalLogs.append("[Execution Time: ").append(String.valueOf(elapsedTime)).append("ns] [Database Stats: Total tables in Database -> ").append(String.valueOf(totalTables)).append("]\n");
        } else {
            generalLogs.append("[Execution Time: ").append(String.valueOf(elapsedTime)).append(
                    "ns] [Database Stat: ").append("No Database was selected! Database " +
                    "stats can not be retrieved").append("]\n");
        }
        if(invalidQuery) throw new InvalidSyntaxException("Invalid Query!");
    }
}
