package types;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import models.Row;
import models.StoredTable;
import models.Table;

public class XMLTable implements StoredTable {

	private static final Path root = Paths.get("db", "tables");
	private static Path flat;
	private static Document doc;

	public XMLTable(String name, List<String> columns) {

		try {
			Files.createDirectories(root);

			flat = root.resolve(name + ".xmlt");
			if(Files.notExists(flat))
				Files.createFile(flat);

			doc = DocumentHelper.createDocument();
			var base = doc.addElement("table");
			var node = base.addElement("columns");
			for(String i : columns) {
				node.addElement(i);
			}
			base.addElement("rows");
			flush();


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public XMLTable(String name) {
		try {
			Files.createDirectories(root);

			flat = root.resolve(name + ".xmlt");
			if(Files.notExists(flat))
				Files.createFile(flat);

			try {
				var reader = new SAXReader().read(flat.toFile());
				doc = reader;
				flush();
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			flush();
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException();
		}
	}

	@Override
	public void clear() {
		var root = doc.getRootElement();
		var rows = root.element("rows");
		rows.clearContent();
	}

	@Override
	public void flush() {
		try {
			XMLWriter writer = new XMLWriter(new FileWriter(flat.toFile()));
			writer.write(doc);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public List<Object> put(String key, List<Object> fields) {
		if(fields.size() != this.degree()-1) {
			throw new IllegalArgumentException();
		}
		var rows = new ArrayList<Row>();
		var root = doc.getRootElement();
		var r = root.element("rows");
		for(var i : r.elements()) {
			if(i.attribute("key").getText().equals(key)) {
				var temp = i;
				root.element("rows").remove(i);
				r.add(this.toElement(key, fields));

				flush();
				return fieldsOf(temp);
			}
		}
		r.add(this.toElement(key, fields));
		flush();
		return null;
	}

	@Override
	public List<Object> get(String key) {
		var rows = new ArrayList<Row>();
		var root = doc.getRootElement();
		var r = root.element("rows");
		for(var i : r.elements()) {
			if(i.attribute("key").getText().equals(key)) {
				return this.fieldsOf(i);
			}
		}
		return null;
	}

	@Override
	public List<Object> remove(String key) {
		var root = doc.getRootElement();
		var r = root.element("rows");
		for(var i : r.elements()) {
			if(i.attribute("key").getText().equals(key)) {
				List<Object> temp = this.fieldsOf(i);
				root.element("rows").remove(i);
				flush();
				return temp;
			}
		}
		return null;
	}

	@Override
	public int degree() {
		var list = this.columns();
		return list.size();
	}

	@Override
	public int size() {
		var root = doc.getRootElement();
		var size = root.element("rows");
		return size.nodeCount();
	}

	@Override
	public int hashCode() {
		int hashCode =0;

		for(Row i : getRows()) {
			hashCode += i.hashCode();
		}
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Table && obj.hashCode() == this.hashCode())
			return true;
		return false;
	}

	@Override
	public Iterator<Row> iterator() {
		var rows = new ArrayList<Row>();
		var root = doc.getRootElement();
		var r = root.element("rows");
		for(var i : r.elements()) {
			rows.add(new Row(this.keyOf(i), this.fieldsOf(i)));
		}
		return rows.iterator();

	}

	@Override
	public String name() {
		var file = XMLTable.flat.getFileName().toString();
		String[] name = file.split("\\.");
		return name[0];
	}

	@Override
	public List<String> columns() {
		List<String> names = new ArrayList<String>();

		try {
			var reader = new SAXReader().read(flat.toFile());
			var root = reader.getRootElement();
			var cols = root.element("columns");
			for(var child: cols.elements()) {
				names.add(child.getName());
			}
			return names;
		} catch (DocumentException e) {
			e.printStackTrace();
		}

		return names;
	}

	@Override
	public String toString() {
		return this.toTabularView(getRows(), false);
	}

	public Element toElement(String key, List<Object> fields) {
		var row = DocumentHelper.createElement("row");
		row.addAttribute("key", key);
		for(var i : fields) {
			row.addElement(getType(i)).addText(encodeField(i));
		}
		return row;
	}

	public String keyOf(Element elems) {
		return elems.attributeValue("key");
	}

	public List<Object> fieldsOf(Element elems){
		List<Object> fields = new ArrayList<Object>();
		for(var i : elems.elements()) {
			fields.add(decodeField(i.getText()));
		}
		return fields;
	}



	public String getType(Object input) {
		if(input != null) {
			if(input instanceof String)
			{
				return "String";
			}
			else if(input instanceof Boolean)
			{
				return "Boolean";
			}
			else if(input instanceof Integer)
			{
				return "Integer";
			}
		}
		return "null";
	}

	public List<Row> getRows(){
		List<Row> rows = new ArrayList<Row>();
		var index = doc.getRootElement().element("rows").elements();
		for(var i : index) {
			Row n = new Row(i.attribute("key").getText(), fieldsOf(i));
			rows.add(n);
		}
		return rows;

	}

	public static String encodeField(Object obj) {
		if(obj == null) {
			return "null";
		}
		String result = obj.toString();
		if(result.equals(" ")) {
			return result = "\"" + result + "\"";
		}
		result = obj.toString().strip();
		if(obj instanceof String) {
			return result = "\"" + result + "\"";
		}
		else if(obj instanceof Boolean || obj instanceof Integer || obj instanceof Double) {
			return result =obj.toString();
		}
		else
			throw new IllegalArgumentException("The given field is unrecognizable");
	}

	public Object decodeField(Object obj) {
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

	public boolean isInteger(String test) {
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

	public boolean isDouble(String test) {
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
}
