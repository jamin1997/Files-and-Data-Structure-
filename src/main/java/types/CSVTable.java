package types;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

import models.Row;
import models.StoredTable;
import models.Table;

public class CSVTable implements StoredTable {

	private static final Path root = Paths.get("db", "tables");
	private static Path flat;


	public CSVTable(String name, List<String> columns) {
		try {
			Files.createDirectories(root);

			flat = root.resolve(name + ".csv");
			if(Files.notExists(flat))
				Files.createFile(flat);

			var header = String.join(", ", columns);
			List<String> records = new ArrayList<String>();
			records.add(header);
			Files.write(flat, records);

		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	public CSVTable(String name) {
		try {
			Files.createDirectories(root);

			flat = root.resolve(name + ".csv");
			if(Files.notExists(flat))
				Files.createFile(flat);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String encodeField(Object obj) {
		if(obj == null) {
			return "null";
		}
		String result = obj.toString().strip();
		if(obj instanceof String) {
			return result = "\"" + result + "\"";
		}
		else if(obj instanceof Boolean || obj instanceof Integer || obj instanceof Double) {
			return result =obj.toString();
		}
		else
			throw new IllegalArgumentException("The given field is unrecognizable");
	}

	public static Object decodeField(Object obj) {
		String result = obj.toString();
		result = result.strip();
		if(result.equals("null")) {
			return null;
		}
		else if(result.startsWith("\"") && result.endsWith("\"")) {
			result = result.substring(result.indexOf("\"")+1);
			result = result.substring(0, result.indexOf("\""));
			return result;
		}
		else if(result.equalsIgnoreCase("true")) {
			return true;
		}
		else if(result.equalsIgnoreCase("false")) {
			return false;
		}
		else if(isInteger(result)) {
			return Integer.parseInt(result);
		}
		else if(isDouble(result)) {
			return Double.parseDouble(result);
		}
		else
			throw new IllegalArgumentException("The given field is unrecognizable" + " " + obj);

	}

	public static String encodeRow(Row made) {
		StringJoiner sj = new StringJoiner(",");
		sj.add(encodeField(made.key()));
		for(int i =0; i< made.fields().size(); i++) {
			sj.add(encodeField(made.fields().get(i)));
		}
		return sj.toString();
	}

	public static Row decodeRow(String record) {
		List<String> split = List.of(record.split(","));
		String key = decodeField(split.get(0).trim()).toString();
		List<Object> fields = new ArrayList<Object>();
		for(int i =1; i < split.size(); i++) {
			fields.add(decodeField(split.get(i).trim()));
		}
		Row made = new Row(key, fields);
		return made;	
	}

	public static boolean isInteger(String test) {
		int dash_counter = 0;
		for(int i =0; i< test.length();i++) {
			if(dash_counter > 1) {
				return false;
			}
			if(test.charAt(i) == '-' && i <test.length()-1) {
				dash_counter++;
				continue;
			}
			if(Character.isDigit(test.charAt(i)) == false) {
				return false;
			}
		}
		return true;
	}

	public static boolean isDouble(String test) {
		int decimal = 0;
		for(int i =0; i< test.length();i++) {
			if(test.charAt(i) == '-') {
				i++;
				continue;
			}
			if(test.charAt(i) == '.' && decimal < 1) {
				i++;
				decimal++;
				continue;
			}
			if(Character.isDigit(test.charAt(i)) == false) {
				return false;
			}
			if(decimal >1) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void clear() {
		try {
			List<String> records = Files.readAllLines(this.flat);
			records.subList(0, 1);
			Files.write(this.flat, records);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<Object> put(String key, List<Object> fields) {
		try {
			List<String> records = Files.readAllLines(flat);
			var list = Arrays.asList(records.get(0).split(","));
			if(fields.size() != list.size()-1) {
				throw new IllegalArgumentException("Degree of new Row does not match table");
			}
			List<Row> temp = new ArrayList<Row>();
			for(int index = 1; index <records.size();index++) {
				temp.add(decodeRow(records.get(index)));
			}
			Row made = new Row(key, fields);
			String row = encodeRow(made);
			for(int i =0; i<temp.size();i++) {
				Row here = temp.get(i);
				if(here.key().equals(key)) {
					records.remove(i+1);
					records.add(1, row);
					Files.write(flat, records);
					return here.fields();
				}
			}
			records.add(row);
			Files.write(flat, records);
			return null;

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public List<Object> get(String key) {
		try {
			List<String> records = Files.readAllLines(flat);
			List<Row> temp = new ArrayList<Row>();
			for(int index = 1; index <records.size();index++) {
				temp.add(decodeRow(records.get(index)));
			}
			for(int i =0; i<temp.size();i++) {
				Row here = temp.get(i);
				if(here.key().equals(key)) {
					records.remove(i+1);
					records.add(1, encodeRow(temp.get(i)));
					Files.write(flat, records);
					return here.fields();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Object> remove(String key) {
		try {
			//read all lines and decode them into a new List
			List<String> records = Files.readAllLines(flat);
			List<Object> temp = new ArrayList<Object>();
			for(int index = 1; index <records.size();index++) {
				temp.add(decodeRow(records.get(index)));
			}
			for(int i =0 ; i < temp.size(); i++) {
				if(((Row) temp.get(i)).key().equals(key)) {
					List<Object> results = new ArrayList<Object>();
					int z = ((Row) temp.get(i)).fields().size();
					for(int j = 0; j<z; j++)
						results.add(((Row) temp.get(i)).fields().get(j));
					records.remove(i+1);
					Files.write(flat, records);
					return results;
				}
			}
			return null;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int degree() {
		List<String> records;
		try {
			records = Files.readAllLines(flat);
			records.subList(0, 1);
			var list = Arrays.asList(records.get(0).split(", "));
			int degree =0;
			for(String i : list) {
				degree++;
			}
			return degree;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int size() {
		List<String> records;
		int size =0;
		try {
			records = Files.readAllLines(flat);
			records.remove(0);

			for(String i : records) {
				size++;
			}
			return size;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int hashCode() {
		int sum =0;
		try {
			Row[] array = new Row[Files.readAllLines(flat).size()];
			List<String> reader = Files.readAllLines(flat);
			for(int i =1; i<Files.readAllLines(flat).size(); i++) {
				array[i] = decodeRow(reader.get(i));
				sum += array[i].hashCode();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sum;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Table && obj.hashCode() == this.hashCode())
			return true;
		return false;
	}

	@Override
	public Iterator<Row> iterator() {
		try {
			List<String> records = Files.readAllLines(flat);
			List<Row> temp = new ArrayList<Row>();
			for(int index = 1; index <records.size();index++) {
				temp.add(decodeRow(records.get(index)));
			}
			return temp.iterator();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return iterator();		
	}

	@Override
	public String name() {
		var file = CSVTable.flat.getFileName().toString();
		String[] name = file.split("\\.");
		return name[0];
	}

	@Override
	public List<String> columns() {
		try {
			List<String> records = List.copyOf(Files.readAllLines(flat));
			List<String> results = new ArrayList<String>();
			var i = records.get(0).split(",");
			for(String j : i)
				results.add(j.strip());
			return results;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;		
	}

	@Override
	public String toString() {
		List<Row> array = new ArrayList<Row>();
		try {
			List<String> records = List.copyOf(Files.readAllLines(flat));
			for(int index = 1; index <records.size();index++) {
				array.add(decodeRow(records.get(index)));
			}
			return this.toTabularView(array, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return this.toTabularView(array, true);
	}
	public static CSVTable fromText(String name, String text) {

		try {
			Files.createDirectories(root);
			flat = root.resolve(name + ".csv");

			if(Files.notExists(flat))
				Files.createFile(flat);

			Files.write(flat, text.getBytes());

		} catch (Exception e) {
			e.printStackTrace();
		}
		return new CSVTable(name);
	}
}
