package ERDCreation;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class ERD {
    private static String ROOT_PATH = "src/main/resources/";
    private static Formatter fmtFile;

    public void createERD(String dataBaseName, FileWriter eventLogs, String Username) throws Exception {
        File databaseFolder = new File(ROOT_PATH + "databases/" + dataBaseName);
        File[] tables = databaseFolder.listFiles();
        if (!databaseFolder.exists()) {
            System.out.println("The Database with Name " + dataBaseName + " does not exist");
        }
        if (tables.length < 1) {
            System.out.println("The Tables does not exist in " + dataBaseName);
        }

        File erdDirectory = new File(ROOT_PATH + "/ERD");

        if (erdDirectory.mkdir()) {
            System.out.println("ERD folder created!");
        }
        // ERD file for particular schema
        String tableFilePath = erdDirectory + "/" + dataBaseName + ".txt";
        File erdFile = new File(tableFilePath);
        if (erdFile.createNewFile()) {
            System.out.println("ERD file created!");
        }

        fmtFile = new Formatter(new FileOutputStream(tableFilePath));


        for (File table : tables) {
            String tableName = table.getName();
            String tablePath = table.getPath();
            if (tableName.equalsIgnoreCase("metaData.txt")) {
                makeERD(dataBaseName, tablePath, eventLogs, Username);
            }
        }
        fmtFile.close();
    }

    private static void makeERD(String dataBaseName, String tablePath, FileWriter eventLogs, String Username) throws IOException {
        File metaTable = new File(tablePath);
        BufferedReader metaData = new BufferedReader(new FileReader(metaTable));
        eventLogs.append("Metadata successfully fetched of table: ").append(dataBaseName).append(" while generating ERD").append("\n");
        String metaLine;

        while ((metaLine = metaData.readLine()) != null) {
            String[] metaSplit = metaLine.split("\\(");
            String tableName = "Table Name :".concat(metaSplit[0]);
            fmtFile.format("===========================================================================================================================================================\n");
            System.out.println("===========================================================================================================================================================");
            fmtFile.format("                                                                        " + tableName + "                                                                      \n");
            System.out.println("                                                                        " + tableName + "                                                                      ");
            fmtFile.format("-----------------------------------------------------------------------------------------------------------------------------------------------------------\n");

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------------------------");
            fmtFile.format("%20s%20s%20s%20s%20s%20s%20s\n", "Columns |", "Data Type |", "Primary Key |", "Foreign Key |", "Foreign Column |", "Foreign Table |", "Cardinality | \n");
            System.out.format("%20s%20s%20s%20s%20s%20s%20s\n", "Columns |", "Data Type |", "Primary Key |", "Foreign Key |", "Foreign Column |", "Foreign Table |", "Cardinality |");
            fmtFile.format("\n");
            System.out.println();
            String[] tableColumnsString = metaLine.split(metaSplit[0]);
            String[] allColumnDetails = tableColumnsString[1].split(",");

            int foreignKeyref=-1;
            for (String columnDetails : allColumnDetails) {
                String dataType = "";
                String column = "";
                String size = "-";
                String primaryKey = "-";
                String foreignKey = "-";
                String foreignColumn = "-";
                String foreignTable = "-";
                String cardinality = "-";
                foreignKeyref++;
                //For Foreign Key

                if (columnDetails.contains(".")) {
                    File tableData = new File(ROOT_PATH + "databases/" + dataBaseName + "/" + metaSplit[0] + ".txt");
                    BufferedReader tableLines = new BufferedReader(new FileReader(tableData));
                    String[] tableColumnData;
                    String tableLine;
                    List<String> keyValues = new ArrayList<>();

                    while ((tableLine = tableLines.readLine()) != null) {
                        tableColumnData = tableLine.split("\\|");
                        keyValues.add(tableColumnData[foreignKeyref]);
                    }
                    Set<String> uniqueKeyValues = new HashSet<String>(keyValues);
                    if(uniqueKeyValues.size()== keyValues.size()) {
                        cardinality = "1:1";
                    } else {
                        cardinality = "1:N";
                    }
                    String[] columnDetailsData = columnDetails.split(Pattern.quote("."));
                    foreignTable = columnDetailsData[0];
                    column = columnDetailsData[1];
                    dataType = "int";
                    if (foreignTable.contains("#")) {
                        foreignKey = "FK";
                        column = column.replaceAll("\\)", "");
                        foreignColumn = column.replace(")", "");
                        foreignTable = foreignTable.replaceAll("#", "");
                    }
                } else {
                    //For All
                    String[] columnDetailsData = columnDetails.split(":");
                    column = columnDetailsData[0];
                    dataType = columnDetailsData[1];
                    String[] dataTypeSize = dataType.split("\\(");
                    if (dataTypeSize.length > 1) {
                        dataType = dataTypeSize[0];
                        size = dataTypeSize[1];
                        size = size.replace(")", "");
                    }
                    dataType = dataType.replace(")", "");
                    if (column.contains("$")) {
                        primaryKey = "PK";
                        column = column.replaceAll("[$(]", "");
                    }
                }
                fmtFile.format("%20s%20s%20s%20s%20s%20s%20s\n", column.concat(" |"), dataType.concat(" |"), primaryKey.concat(" |"), foreignKey.concat(" |"), foreignColumn.concat(" |"), foreignTable.concat(" |"), cardinality.concat(" |"));
                System.out.format("%20s%20s%20s%20s%20s%20s%20s\n", column.concat(" |"), dataType.concat(" |"), primaryKey.concat(" |"), foreignKey.concat(" |"), foreignColumn.concat(" |"), foreignTable.concat(" |"), cardinality.concat(" |"));
            }
        }
        eventLogs.append("[User: ").append(Username).append("] ERD Generated Successfully for Database :").append(dataBaseName).append("\n");
        fmtFile.format("===========================================================================================================================================================");
        System.out.println("===========================================================================================================================================================");
        fmtFile.format("\n");
        System.out.println("ERD Created Successfully of Database : " + dataBaseName);
    }

}
