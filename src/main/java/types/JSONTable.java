package types;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Row;
import models.StoredTable;
import models.Table;

public class JSONTable implements StoredTable {

	private static final Path root = Paths.get("db", "tables");
	private static Path flat;
	private ObjectNode tree;
	private static final JsonMapper mapper = new JsonMapper();

	public JSONTable(String name, List<String> columns) {
		try {
			Files.createDirectories(root);

			flat = root.resolve(name + ".json");
			if(Files.notExists(flat))
				Files.createFile(flat);

			tree = mapper.createObjectNode();
			var meta = tree.putObject("metadata");
			tree.putObject("data");
			meta.putPOJO("columns", columns);
			flush();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	public JSONTable(String name) {
		try {
			Files.createDirectories(root);

			flat = root.resolve(name + ".json");
			if(Files.notExists(flat))
				Files.createFile(flat);	

			tree = (ObjectNode) mapper.readTree(flat.toFile());

		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException();
		}
	}


	@Override
	public void clear() {
		tree.remove("data");
		tree.putObject("data");
		flush();
	}

	@Override
	public void flush() {
		try {
			mapper.writerWithDefaultPrettyPrinter().writeValue(flat.toFile(), tree);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<Object> put(String key, List<Object> fields) {
		if(fields.size() != this.degree()-1) {
			throw new IllegalArgumentException();
		}
		if(mapper.convertValue(tree.get("data").get(key), List.class) != null) {
			var temp = mapper.convertValue(tree.get("data").get(key), List.class);
			try {
				var node = mapper.treeToValue(tree.get("data"),ObjectNode.class);
				node.putPOJO(key, fields);
			} catch (JsonProcessingException | IllegalArgumentException e) {
				e.printStackTrace();
			}
			flush();
			return temp;
		}
		try {
			var node = mapper.treeToValue(tree.get("data"),ObjectNode.class);
			node.putPOJO(key, fields);
		} catch (JsonProcessingException | IllegalArgumentException e) {
			e.printStackTrace();
		}
		flush();


		return null;
	}


	@Override
	public List<Object> get(String key) {
		return mapper.convertValue(tree.get("data").get(key), List.class);
	}

	@Override
	public List<Object> remove(String key) {
		if(mapper.convertValue(tree.get("data").get(key), List.class) != null) {
			var temp = mapper.convertValue(tree.get("data").get(key), List.class);
			((ObjectNode) tree.get("data")).remove(key);
			flush();
			return temp;
		}
		return null;
	}

	@Override
	public int degree() {
		return mapper.convertValue(tree.get("metadata").get("columns"), List.class).size();
	}

	@Override
	public int size() {
		return tree.get("data").size();
	}

	@Override
	public int hashCode() {
		int hashCode =0;
		for(Row i : this) {
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
		var fields = tree.get("data").fields();
		var rows = new ArrayList<Row>();
		while(fields.hasNext()) {
			var next = fields.next();
			rows.add(new Row(next.getKey(), mapper.convertValue(next.getValue(), List.class)));
		}
		return rows.iterator();
	}

	@Override
	public String name() {
		var file = JSONTable.flat.getFileName().toString();
		String[] name = file.split("\\.");
		return name[0];
	}

	@Override
	public List<String> columns() {
		return mapper.convertValue(tree.get("metadata").get("columns"), List.class);
	}

	@Override
	public String toString() {
		List<Row> rows = new ArrayList<Row>();
		for(Row i : this) {
			rows.add(i);
		}
		return this.toTabularView(rows, false);
	}
}
