package Query.Processor;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryProcessor {

    public String currentDatabase = null;
    String ROOT_PATH = "src/main/resources/databases/";
    Pattern META_DATA = Pattern.compile("(.*)\\(((.*):(.*)(,?))*\\)", Pattern.CASE_INSENSITIVE);
    HashMap<String, HashMap<String, String>> metaData = new HashMap<>();
    HashMap<String, ArrayList<List<String>>> tableData = new HashMap<>();
    HashMap<String, List<String>> tableHeaders = new HashMap<>();

    public void createDatabase(String dbName) {
        File db = new File(ROOT_PATH + dbName);
        if (db.exists()) {
            System.out.println("Database " + dbName + " already exists");
            throw new FileSystemAlreadyExistsException();
        } else {
            if (db.mkdirs()) {
                System.out.println("Database " + dbName + " created");
                createMetaDataFile(dbName);
            }
        }
    }

    public void createMetaDataFile(String dbName) {
        try {
            File t = new File(ROOT_PATH + dbName + "/metaData.txt");
            t.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void useDatabase(String dbName) {
        File db = new File(ROOT_PATH + dbName);
        if (db.exists()) {
            currentDatabase = dbName;
            loadCaches();
            System.out.println("Database changed to " + dbName);
        } else {
            System.out.println("Database " + dbName + " does not exists");
        }
    }

    private void loadCaches() {
        loadMetaDataFile();
        loadTableData();
    }

    private void loadTableData() {
        String dir = ROOT_PATH + currentDatabase;
        try {
            List<File> filesInFolder = Files.walk(Paths.get(dir))
                    .filter(Files::isRegularFile).filter(Files -> !Files.endsWith("metaData.txt"))
                    .map(Path::toFile)
                    .collect(Collectors.toList());
            for (File file : filesInFolder) {
                readTableData(file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readTableData(File file) {
        try {
            Scanner myReader = new Scanner(file);
            ArrayList<List<String>> tableRowData = new ArrayList<>();
            while (myReader.hasNextLine()) {
                tableRowData.add(Arrays.asList(myReader.nextLine().split("\\|")));
            }
            tableData.put(file.getName().split("\\.")[0], tableRowData);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void loadMetaDataFile() {
        File file = new File(ROOT_PATH + currentDatabase + "/metaData.txt");
        try {
            Scanner myReader = new Scanner(file);
            while (myReader.hasNextLine()) {
                String tableData = myReader.nextLine();
                Matcher matcher = META_DATA.matcher(tableData);
                if (matcher.find()) {
                    String[] columns = matcher.group(2).split(",");
                    HashMap<String, String> columnData = new HashMap<>();
                    List<String> tableHeaders = new ArrayList<>();
                    for (String column : columns) {
                        String[] colAttrs = column.split(":");
                        if (colAttrs.length < 2) {
                            String[] foreignKey = column.split("\\.");
                            columnData.put(foreignKey[1], "");
                            tableHeaders.add(foreignKey[1]);
                        } else {
                            columnData.put(colAttrs[0], colAttrs[1]);
                            tableHeaders.add(colAttrs[0]);
                        }
                    }
                    metaData.put(matcher.group(1), columnData);
                    loadTableHeaders(matcher.group(1), tableHeaders);
                }
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void loadTableHeaders(String tableName, List<String> tableHeaders) {
        List<String> filteredTableHeaders = new ArrayList<>();
        for (String header : tableHeaders) {
            filteredTableHeaders.add(header.replace("$", ""));
        }
        this.tableHeaders.put(tableName, filteredTableHeaders);
    }

    public void dropDatabase(String dbName) {

        File t = new File(ROOT_PATH + dbName);
        if (t.exists()) {
            try {
                FileUtils.deleteDirectory(t);
                currentDatabase = null;
                metaData = new HashMap<>();
                tableData = new HashMap<>();
                tableHeaders = new HashMap<>();
                System.out.println("Dropped Database " + dbName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Database " + dbName + " does not exists");
        }
    }

    public void createTable(String tableName, HashMap<String, String> columnData, FileWriter queryLogs, Timestamp ts, String Username,String query) {
        if (currentDatabase != null) {
            try {
                queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                                "[Database: ").append(currentDatabase).append(" ] [Table: ").append(tableName).append("] [Query Operation: insert] [Query: ").append(query)
                                .append("] [Query Type: Valid]\n");
                File t = new File(ROOT_PATH + currentDatabase + "/" + tableName + ".txt");
                if (t.createNewFile()) {
                    System.out.println("Table " + tableName + " created successfully");
                    updateMetaDataFile(tableName, columnData);
                    loadCaches();
                } else {
                    System.out.println("Table " + tableName + " already exists");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Switch to a database before creating table");
        }
    }

    private void updateMetaDataFile(String tableName, HashMap<String, String> columnData) {
        try {
            FileWriter fw = new FileWriter(ROOT_PATH + currentDatabase + "/metaData.txt", true);
            ArrayList<String> colData = new ArrayList<>();
            String foreignKey = null;
            if (columnData.containsKey("primary_key")) {
                String primaryKey = columnData.get("primary_key");
                colData.add("$" + primaryKey + ":" + columnData.get(primaryKey));
                columnData.remove("primary_key");
                columnData.remove(primaryKey);
            }
            if (columnData.containsKey("foreign_key")) {
                foreignKey = "#" + columnData.get("foreign_key");
                columnData.remove("foreign_key");
            }
            for (HashMap.Entry<String, String> entry : columnData.entrySet()) {
                colData.add(entry.getKey() + ":" + entry.getValue());
            }
            if (foreignKey != null) colData.add(foreignKey);
            String metaData = tableName + "(" + String.join(",", colData) + ")\n";
            fw.write(metaData);
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

        public void dropTable(String tableName,FileWriter queryLogs, Timestamp ts, String Username,String query) throws IOException {
        if (currentDatabase != null) {
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                            "[Database: ").append(currentDatabase).append(" ] [Table: ").append(tableName).append("] [Query Operation: drop table] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            File t = new File(ROOT_PATH + currentDatabase + "/" + tableName + ".txt");
            if (t.exists()) {
                if (t.delete()) {
                    System.out.println("Dropped Table " + tableName);
                    try {
                        removeFromMetaData(tableName);
                        loadCaches();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Could not drop table " + tableName);
                }
            } else {
                System.out.println("Table " + tableName + " does not exists");
            }
        } else {
            System.out.println("Switch to a database before creating table");
        }
    }

    public void removeFromMetaData(String tableName) throws IOException {
        File file = new File(ROOT_PATH + currentDatabase + "/metaData.txt");
        List<String> out = Files.lines(file.toPath())
                .filter(line -> !line.startsWith(tableName))
                .collect(Collectors.toList());
        Files.write(file.toPath(), out, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void writeTableData(String tableName, HashMap<String, String> rowData, FileWriter queryLogs, Timestamp ts, String Username, String query) {
        try {
            if (!rowData.isEmpty()) {
                if (rowData.size() == tableHeaders.get(tableName).size()) {
                    System.out.println("Inserted 1 row into " + tableName);
                    List<String> rowsData = new ArrayList<>();
                    for (String col : tableHeaders.get(tableName)) {
                        rowsData.add(rowData.get(col));
                    }
                    tableData.get(tableName).add(rowsData);
                } else {
                    System.out.println("Some values missing for inserting to table!");
                }
            }
            FileWriter fw = new FileWriter(ROOT_PATH + currentDatabase + "/" + tableName + ".txt");
            ArrayList<String> rowsData = new ArrayList<>();
            for (List<String> row : tableData.get(tableName)) {
                rowsData.add(String.join("|", row));
            }
            String data = String.join("\n", rowsData);
            fw.write(data);
            fw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void showTable(String tableName) {
        if (this.tableHeaders.containsKey(tableName)) {
            List<String> tableHeaders = this.tableHeaders.get(tableName);
            for (String header : tableHeaders) {
                System.out.printf("%20s %2s", header, "|");
            }
            System.out.println();
            ArrayList<List<String>> rows = tableData.get(tableName);
            for (List<String> row : rows) {
                for (String attr : row) {
                    System.out.printf("%20s %2s", attr, "|");
                }
                System.out.println();
            }
            System.out.println();
        } else {
            System.out.println("Table " + tableName + " does not exist in " + currentDatabase);
        }
    }

    public void showSelectData(String tableName, String attrName, String attrVal) {
        if (metaData.containsKey(tableName)) {
            List<String> tableHeaders = this.tableHeaders.get(tableName);
            for (String header : tableHeaders) {
                System.out.printf("%20s %2s", header, "|");
            }
            System.out.println();
            int attrIndex = this.tableHeaders.get(tableName).indexOf(attrName);
            ArrayList<List<String>> rows = tableData.get(tableName);
            for (List<String> row : rows) {
                if (row.get(attrIndex).contains(attrVal)) {
                    for (String attr : row) {
                        System.out.printf("%20s %2s", attr, "|");
                    }
                    System.out.println();
                }
            }
        }
    }

    public void deleteRow(String tableName, String attrName, String attrVal,FileWriter queryLogs,Timestamp ts, String Username, String query) throws IOException {
        if (metaData.containsKey(tableName)) {
            int attrIndex = this.tableHeaders.get(tableName).indexOf(attrName);
            ArrayList<List<String>> updatedRows = new ArrayList<>();
            int deletedRowsCount = 0;
            for (List<String> row : tableData.get(tableName)) {
                if (!row.get(attrIndex).contains(attrVal)) {
                    updatedRows.add(row);
                }else{
                    deletedRowsCount++;
                }
            }
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " +
                            "[Database: ").append(currentDatabase).append(" ] [Table: ").append(tableName).append("] [Query Operation: alter] [Query: ").append(query)
                    .append("] [Query Type: Valid]\n");
            System.out.println("Deleted "+deletedRowsCount+" row(s) in "+tableName);
            this.tableData.put(tableName, updatedRows);
            writeTableData(tableName, new HashMap<>(),queryLogs,ts,Username,query);
        }
    }

    public void updateRow(String tableName, String attrName, String newAttrVal, String condName, String condAttrVal,FileWriter queryLogs,Timestamp ts, String Username, String query) throws IOException {
        if (metaData.containsKey(tableName)) {
            int condAttrIndex = this.tableHeaders.get(tableName).indexOf(condName);
            ArrayList<List<String>> updatedRows = new ArrayList<>();
            int updatedRowsCount = 0;
            for (List<String> row : tableData.get(tableName)) {
                if (row.get(condAttrIndex).contains(condAttrVal)) {
                    int attrIndex = this.tableHeaders.get(tableName).indexOf(attrName);
                    updatedRowsCount++;
                    row.set(attrIndex, newAttrVal);
                }
                updatedRows.add(row);
            }
            queryLogs.append("[Timestamp: ").append(String.valueOf(ts)).append(" ] [User: ").append(Username).append("] " + "[Database: ").append(currentDatabase).append(" ] [Table: ").append(tableName).append("] [Query Operation: alter] [Query: ").append(query).append("] [Query Type: Valid]\n");
            System.out.println("Updated " + updatedRowsCount + " row(s) in "+tableName);
            this.tableData.put(tableName, updatedRows);
            writeTableData(tableName, new HashMap<>(),queryLogs,ts ,Username,query);
        }
    }

    public void showDatabases() {
        File[] databases = new File(ROOT_PATH).listFiles(File::isDirectory);
        if(databases != null) {
            for (File database : databases) {
                System.out.println(database.getName());
            }
        }
    }

    public void showTables() {
        if (currentDatabase != null){
            try {
                List<File> filesInFolder = Files.walk(Paths.get(ROOT_PATH+currentDatabase))
                        .filter(Files::isRegularFile).filter(Files -> !Files.endsWith("metaData.txt"))
                        .map(Path::toFile)
                        .collect(Collectors.toList());
                for (File file : filesInFolder) {
                    System.out.println(file.getName().split("\\.")[0]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            System.out.println("Switch to a database before creating table");
        }
    }
}
