package FileHelpers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class fileHelper {

	public static void createDump(String dbLocation) throws IOException {
		dbLocation = dbLocation + "/";
		String metaStr = dbLocation + "metaData.txt";

		ArrayList<String> tables = new ArrayList<String>();
		String createTableString = "";
		try {
			File myObj = new File(metaStr);
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();

				String[] s = data.split("\\(");
				String tableName = s[0];// getting table name
				tables.add(tableName);

				String attributes = s[1].split("\\)")[0]; // getting atributes of table

				createTableString += "\nCreate table " + tableName + " ( \n";
				String pk = null;
				ArrayList<String> fk = new ArrayList<String>();
				ArrayList<String> fkReference = new ArrayList<String>();

				for (String w : attributes.split(",")) { // getting atributes one by one of table

					if (w.contains("$")) {
						pk = w.split(":")[0].replace("$", " ");
					}
					if (w.contains("#")) {
						w=w+":int";
						fkReference.add(w.split("\\.")[0].replace("#", " "));
						fk.add(w.split("\\.")[1].split(":")[0]);

						createTableString += w.replace("$", " ").replace(":", " ").replace("#", "  ").split("\\.")[1]
								+ ", \n";

					} else
						createTableString += w.replace("$", " ").replace(":", " ").replace("#", "  ") + ", \n";

				}
				if (pk != null) {
					createTableString += "PRIMARY KEY (" + pk + "), \n";
				}
				if (fk.size() > 0) {

					for (int i = 0; i < fk.size(); i++) {
						createTableString += " Foreign KEY (" + fk.get(i) + ") REFERENCES " + fkReference.get(i) + " ( "
								+ fk.get(i) + " ), \n";

					}
				}
				createTableString = createTableString.trim();
				createTableString = createTableString.substring(0, createTableString.length() - 1) + (" );");
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		try {
			writeToFile(createTableString,"create.txt");
		} catch (Exception e) {
			e.printStackTrace();
		}

		String tableString = "";
		String data;
		for (String t : tables) {
			tableString = "";
			try {
				File myObj = new File(dbLocation + t + ".sql");
				Scanner myReader = new Scanner(myObj);

				while (myReader.hasNextLine()) {
					data = "";
					data = myReader.nextLine();
					data = data.replace("|", "','");

					tableString += "insert into table " + t + " values ('" + data + "') ;\n";
				}
			} catch (Exception e) {

			}
			writeToFile(tableString,t+".sql");

		}
	}

	private static void writeToFile(String str,String fileName) throws IOException  {
		
		try {
			FileWriter out;
			out = new FileWriter("src/main/resources/dumps/"+fileName);
			out.write(str);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
