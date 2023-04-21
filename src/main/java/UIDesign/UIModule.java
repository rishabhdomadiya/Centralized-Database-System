package UIDesign;

import ERDCreation.*;
import FileHelpers.fileHelper;
import Query.Parser.QueryParser;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Scanner;

public class UIModule {
    private static String ROOT_PATH_LOGS = "src/main/resources/Logs/";
    String ROOT_PATH_CREATE_DUMP = "src/main/resources/databases/";
    QueryParser queryParser;
    public void indexPart() throws Exception {
        Scanner reader = new Scanner(System.in);
        System.out.println("Welcome to our database.");
        System.out.println("...........................");
        System.out.println("Select one option from the following");
        System.out.println("1. Register User");
        System.out.println("2. Login in the database");

        System.out.print("Enter - ");
        int n = reader.nextInt();
        switch (n) {
            case 1:
                registration();
                break;
            case 2:
                login();
                break;

        }
        reader.close();
        System.out.println("...........................");

    }

    public void registration() throws Exception {
        Scanner reader = new Scanner(System.in);
        System.out.println("Enter your ID and password for Registration");
        System.out.print("Enter User ID - ");
        String userId = reader.nextLine();
        System.out.print("Enter Password - ");
        String password = reader.nextLine();
        System.out.print("(Security Question) What's favourite colour? - ");
        String securityAnswer = reader.nextLine();
        System.out.println(userId + password + securityAnswer);

        FileWriter writer = null;
        try {
            writer = new FileWriter("src/main/resources/Users/User_Profile.txt", true);
            writer.write(encryptString(userId) + ",@#" + encryptString(password) + ",@#" + securityAnswer + "\n");
            writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        System.out.println("Thank you for the registration! Please login now.");
        indexPart();
    }

    public void login() throws Exception {
        Scanner reader = new Scanner(System.in);
        System.out.println("Enter your ID and password for Login");
        System.out.print("Enter User ID - ");
        String userId = reader.nextLine();
        System.out.print("Enter Password - ");
        String password = reader.nextLine();
        System.out.print("(Security Question) What's favourite colour? - ");
        String securityAnswer = reader.nextLine();

        boolean isSuccess = authenticate(encryptString(userId), encryptString(password), securityAnswer);
        if (isSuccess) {
            System.out.println("Login Successful!");
            // General Log File
            File generalLogs = new File(ROOT_PATH_LOGS+"Generallogs.txt");
            // Event Log File
            File eventLogs = new File(ROOT_PATH_LOGS+"EventLogs.txt");
            // Query Log File
            File queryLogs = new File(ROOT_PATH_LOGS+"QueryLogs.txt");

            if (!generalLogs.exists()) {
                generalLogs.createNewFile();
                System.out.println("New General Logs created!");
            }

            if (!eventLogs.exists()) {
                eventLogs.createNewFile();
                System.out.println("New Event Logs created!");
            }

            if (!queryLogs.exists()) {
                queryLogs.createNewFile();
                System.out.println("New Query Logs created!");
            }

            // True indicates that data or text will be appended
            FileWriter eventLogsWriter = new FileWriter(eventLogs, true);
            FileWriter generalLogsWriter = new FileWriter(generalLogs, true);
            FileWriter queryLogsWriter = new FileWriter(queryLogs, true);
            queryParser = new QueryParser(eventLogsWriter, generalLogsWriter, queryLogsWriter);
            boolean isExit = true;
            while (isExit) {
                System.out.println("Select one of the below option");
                System.out.println("1. Write Queries\n" +
                        "2. Generate Data Model - Reverse Engineer \n" +
                        "3. Export Structure & Value\n" +
                        "4. Analytics");
                System.out.println("Type Exit to Stop");
                System.out.print("Enter - ");
                String input = reader.nextLine();
                switch (input) {
                    case "1":
                        boolean exit = false;
                        while(!exit) {
                            System.out.print("Enter Query: ");
                            String query = reader.nextLine();
                            if(query.equalsIgnoreCase("exit")){
                                exit=true;
                            }else {
                                try {
                                    queryParser.parseQuery(query, userId);
                                }
                                catch (Exception e){
                                    System.out.println(e.getMessage());
                                }
                            }
                        }
                        break;
                    case "2" :
                        System.out.print("Enter DataBase Name for ERD you want to generate : ");
                        String dataBaseName = reader.nextLine();
                        ERD erd = new ERD();
                        try {
                            erd.createERD(dataBaseName,eventLogsWriter,userId);
                        }catch (Exception e){
                            System.out.println("The Database with the Name Does not Exist");
                        }
                        break;
                    case "3" :
                        System.out.print("Enter DataBase Name for Data Dump you want to generate : ");
                        String dataBase = reader.nextLine();
                        fileHelper fileHelper = new fileHelper();
                        try {
                            fileHelper.createDump(ROOT_PATH_CREATE_DUMP+dataBase);
                            System.out.println("SQL Dump Generated Successfully");
                        }catch (Exception e){
                            System.out.println("The Database with the Name Does not Exist");
                        }
                        break;
                    case "4":
                        System.out.println("Enter the User Name You want to perform analytics for");
                        String username = reader.nextLine();
                        System.out.println("Perform analysis using following commands:");
                        System.out.println("1. count queries <database_name>");
                        System.out.println("2. count update <database_name>");
                        System.out.println("3. count insert <database_name>");
                        System.out.println("4. count delete <database_name>");
                        System.out.println("Write your analysis query here:");
                        String query = reader.nextLine();
                        Analytics analytics = new Analytics();
                        analytics.performAnalysis(query, username);
                        System.out.println();
                        break;
                    case "Exit":
                        eventLogsWriter.close();
                        generalLogsWriter.close();
                        queryLogsWriter.close();
                        isExit = false;
                        break;
                    default :
                        break;

                }
            }


        } else {
            System.out.println("Either UserID or Password is incorrect..");
        }
    }

    public String encryptString(String s) {

        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        messageDigest.update(s.getBytes());
        String stringHash = new String(messageDigest.digest());

        return stringHash;

    }

    public boolean authenticate(String userid, String password, String securityAnswer) {
        String filepath = "src/main/resources/Users/User_Profile.txt";
        Scanner s = null;
        try {
            s = new Scanner(new File(filepath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ArrayList<String> list = new ArrayList<String>();
        while (s.hasNextLine()) {
            list.add(s.nextLine());
        }
        s.close();
        for (String data : list) {
            String[] fieldlist = data.split(",@#");
            for (int i = 0; i < fieldlist.length; i++) {
                if (i % 3 == 0) {
                    if (fieldlist[i].equals(userid) && fieldlist[i + 1].equals(password) && fieldlist[i + 2].equals(securityAnswer)) {
                        return true;
                    }

                }
            }
        }
        return false;
    }
}


