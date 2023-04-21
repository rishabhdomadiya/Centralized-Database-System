package UIDesign;

import Query.Parser.InvalidSyntaxException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Analytics {

    private static final String ROOT_PATH = "src/main/resources";
    static Pattern COUNT_QUERIES = Pattern.compile("Count queries (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern COUNT_UPDATE_QUERIES = Pattern.compile("Count update (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern COUNT_INSERT_QUERIES = Pattern.compile("Count insert (.*);", Pattern.CASE_INSENSITIVE);
    static Pattern COUNT_DELETE_QUERIES = Pattern.compile("Count delete (.*);", Pattern.CASE_INSENSITIVE);
    private static FileWriter fileWriter;
    private static File analysis_directory;

    private static boolean checkDatabaseExists(String dbName) throws FileNotFoundException {
        File database_path = new File(ROOT_PATH + "/databases/" + dbName);
        if (!database_path.exists()) {
            System.out.println("Database " + dbName + " does not exist");
            throw new FileNotFoundException();
        }
        return false;
    }

    private static void countInsertQueries(String dbName, String userName, File file) throws IOException {

        FileReader event_reader = new FileReader("src/main/resources/Logs/QueryLogs.txt");
        fileWriter = new FileWriter(analysis_directory+"/analysis.txt", true);
        BufferedReader bufferedReader = new BufferedReader(event_reader);
        String nextLine;
        String table_name;
        Map<String, Integer> insert_count = new HashMap<>();
        while ((nextLine = bufferedReader.readLine()) != null) {

            if (getInsertQuery(nextLine, dbName, userName)) {
                table_name = getTableName(nextLine);
                if (insert_count.containsKey(table_name)) {
                    insert_count.replace(table_name, insert_count.get(table_name) + 1);
                } else {
                    insert_count.put(table_name, 1);
                }
            }
        }

        if (insert_count.isEmpty()) {
            String output = String.format("User %s performed 0 Insert Operations on database %s", userName, dbName);
            System.out.println(output);

            String analysis_query = "count insert " + dbName + " by " + userName;
            String file_text = String.format("Analysis Query: %s, Result: %s", analysis_query, output);
            fileWriter.write(file_text);

        } else {
            String analysis_query = "count insert " + dbName + " by " + userName;
            fileWriter.write("\nAnalysis Query: %s\n".formatted(analysis_query));
            for (Map.Entry<String, Integer> entry : insert_count.entrySet()) {
                String output = String.format("Total %s Insert operations were performed on %s", entry.getValue(), entry.getKey());
                System.out.println(output);
                String file_text = String.format(" Result: %s,", output);
                if (file.exists()) {
                    fileWriter.append(file_text);
                } else {
                    fileWriter.write(file_text);
                }
            }
        }
        fileWriter.close();

    }

    private static boolean getInsertQuery(String nextLine, String dbName, String userName) {
        nextLine = nextLine.toLowerCase();
        return (nextLine.contains(dbName) && nextLine.contains(userName.toLowerCase()) && nextLine.contains("insert"));
    }

    private static void countQueries(String dbName, String userName, File file) throws IOException {
        int valid_count = 0;
        int invalid_count = 0;
        fileWriter = new FileWriter(analysis_directory+"/analysis.txt", true);
        FileReader event_reader = new FileReader("src/main/resources/Logs/QueryLogs.txt");
        BufferedReader bufferedReader = new BufferedReader(event_reader);
        String nextLine;

        while ((nextLine = bufferedReader.readLine()) != null) {

            if (getUserDatabase(nextLine, dbName, userName)) {
                String queryType = getQueryValidation(nextLine);
                if (queryType.equalsIgnoreCase("valid")) {
                    valid_count++;
                } else if (queryType.equalsIgnoreCase("invalid")) {
                    invalid_count++;
                }
            }
        }

        String output = String.format("User %s submitted %s queries (%s valid and %s invalid) on %s", userName, valid_count + invalid_count, valid_count, invalid_count, dbName);
        System.out.println(output);

        String analysis_query = "count queries " + dbName + " by " + userName;
        String file_text = String.format("\nAnalysis Query: %s, Result: %s", analysis_query, output);
        if (file.exists()) {
            fileWriter.append(file_text);
        } else {
            fileWriter.write(file_text);
        }

        fileWriter.close();
    }

    private static boolean getUserDatabase(String nextLine, String dbName, String userName) {
        boolean contain_database = Pattern.compile(Pattern.quote(dbName), Pattern.CASE_INSENSITIVE).matcher(nextLine).find();
        boolean contain_user = Pattern.compile(Pattern.quote(userName), Pattern.CASE_INSENSITIVE).matcher(nextLine).find();

        return (contain_database && contain_user);
    }

    private static void countUpdateQueries(String dbName, String userName, File file) throws IOException {

        FileReader event_reader = new FileReader("src/main/resources/Logs/QueryLogs.txt");
        fileWriter = new FileWriter(analysis_directory+"/analysis.txt", true);
        BufferedReader bufferedReader = new BufferedReader(event_reader);
        String nextLine;
        String table_name;
        Map<String, Integer> update_count = new HashMap<>();
        while ((nextLine = bufferedReader.readLine()) != null) {

            if (getUpdateQuery(nextLine, dbName, userName)) {
                table_name = getTableName(nextLine);
                if (update_count.containsKey(table_name)) {
                    update_count.replace(table_name, update_count.get(table_name) + 1);
                } else {
                    update_count.put(table_name, 1);
                }
            }
        }

        if (update_count.isEmpty()) {
            String output = String.format("User %s performed 0 Update Operations on database %s", userName, dbName);
            System.out.println(output);

            String analysis_query = "count update " + dbName + " by " + userName;
            String file_text = String.format("\nAnalysis Query: %s, Result: %s", analysis_query, output);
            fileWriter.write(file_text);

        }

        String analysis_query = "count update " + dbName + " by " + userName;
        fileWriter.write("\nAnalysis Query: %s\n".formatted(analysis_query));
        for (Map.Entry<String, Integer> entry : update_count.entrySet()) {
            String output = String.format("Total %s Update operations were performed on %s", entry.getValue(), entry.getKey());
            System.out.println(output);
            String file_text = String.format(" Result: %s,", output);
            if (file.exists()) {
                fileWriter.append(file_text);
            } else {
                fileWriter.write(file_text);
            }
        }
        fileWriter.close();

    }

    private static String getTableName(String nextLine) {
        Pattern pattern = Pattern.compile("\\[Table: (.+?)\\]");
        Matcher matcher = pattern.matcher(nextLine);
        String table_name = null;
        if (matcher.find()) {
            table_name = matcher.group(1);
        }
        return table_name;
    }

    private static String getQueryValidation(String nextLine) {
        Pattern pattern = Pattern.compile("\\[Query Type: (.+?)\\]");
        Matcher matcher = pattern.matcher(nextLine);
        String query = null;
        if (matcher.find()) {
            query = matcher.group(1);
        }
        return query;
    }

    private static boolean getUpdateQuery(String nextLine, String dbName, String userName) {
        nextLine = nextLine.toLowerCase();
        return (nextLine.contains(dbName) && nextLine.contains(userName.toLowerCase()) && nextLine.contains("update"));
    }

    public void performAnalysis(String query, String userName) throws IOException, InvalidSyntaxException {

        File file = new File("analysis.txt");
        boolean database_exists;

        Path path = Path.of(ROOT_PATH + "/Analysis");
        analysis_directory = new File(ROOT_PATH + "/Analysis");
        if (Files.notExists(path)) {
            analysis_directory.mkdir();
        }

        Matcher countMatcher = COUNT_QUERIES.matcher(query);
        Matcher updateMatcher = COUNT_UPDATE_QUERIES.matcher(query);
        Matcher insertMatcher = COUNT_INSERT_QUERIES.matcher(query);
        Matcher deleteMatcher = COUNT_DELETE_QUERIES.matcher(query);
        if (countMatcher.find()) {
            String dbName = countMatcher.group(1);
            database_exists = checkDatabaseExists(dbName);
            if (!database_exists) {
                countQueries(dbName, userName, file);
            }
        } else if (updateMatcher.find()) {
            String dbName = updateMatcher.group(1);
            database_exists = checkDatabaseExists(dbName);
            if (!database_exists) {
                countUpdateQueries(dbName, userName, file);
            }
        } else if (insertMatcher.find()) {
            String dbName = insertMatcher.group(1);
            database_exists = checkDatabaseExists(dbName);
            if (!database_exists) {
                countInsertQueries(dbName, userName, file);
            }
        } else if (deleteMatcher.find()) {
            String dbName = deleteMatcher.group(1);
            database_exists = checkDatabaseExists(dbName);
            if (!database_exists) {
                countDeleteQueries(dbName, userName, file);
            }
        } else {
            throw new InvalidSyntaxException("Incorrect analysis query syntax");
        }

    }

    private void countDeleteQueries(String dbName, String userName, File file) throws IOException {
        FileReader event_reader = new FileReader("src/main/resources/Logs/QueryLogs.txt");
        fileWriter = new FileWriter(analysis_directory+"/analysis.txt", true);
        BufferedReader bufferedReader = new BufferedReader(event_reader);
        String nextLine;
        String table_name;
        Map<String, Integer> delete_count = new HashMap<>();
        while ((nextLine = bufferedReader.readLine()) != null) {

            if (getDeleteQuery(nextLine, dbName, userName)) {
                table_name = getTableName(nextLine);
                if (delete_count.containsKey(table_name)) {
                    delete_count.replace(table_name, delete_count.get(table_name) + 1);
                } else {
                    delete_count.put(table_name, 1);
                }
            }
        }

        if (delete_count.isEmpty()) {
            String output = String.format("User %s performed 0 Delete Operations on database %s", userName, dbName);
            System.out.println(output);

            String analysis_query = "count delete " + dbName + "by " + userName;
            String file_text = String.format("Analysis Query: %s, Result: %s", analysis_query, output);
            fileWriter.write(file_text);

        } else {
            String analysis_query = "count delete " + dbName + " by " + userName;
            fileWriter.write("\nAnalysis Query: %s\n".formatted(analysis_query));
            for (Map.Entry<String, Integer> entry : delete_count.entrySet()) {
                String output = String.format("Total %s Delete operations were performed on %s", entry.getValue(), entry.getKey());
                System.out.println(output);
                String file_text = String.format(" Result: %s,", output);
                if (file.exists()) {
                    fileWriter.append(file_text);
                } else {
                    fileWriter.write(file_text);
                }
            }
        }
        fileWriter.close();

    }

    private boolean getDeleteQuery(String nextLine, String dbName, String userName) {
        nextLine = nextLine.toLowerCase();
        return (nextLine.contains(dbName) && nextLine.contains(userName.toLowerCase()) && nextLine.contains("delete"));
    }
}

