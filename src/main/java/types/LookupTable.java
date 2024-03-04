package types;

import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

import models.Row;
import models.Table;

public class LookupTable implements Table {

	private Row[] array;
	private int degree;
	private String name;
	private List<String> columns;

	// complete
	public LookupTable(String name, List<String> columns) {
		this.degree = columns.size();
		this.name = name;
		this.columns = columns;
		clear();
	}

	// complete
	@Override
	public void clear() {
		array = new Row[52];
	}

	// complete
	private int indexOf(String key) {
		if (key.length() != 1)
			throw new IllegalArgumentException("Key must be length 1");

		char c = key.charAt(0);
		if (c >= 'a' && c <= 'z')
			return c - 'a';
		else if(c >= 'A' && c<='Z')
			return c -'A' + 26;
		else
			throw new IllegalArgumentException("Key must be a lowercase or uppercase letter");
	}

	//  complete
	@Override
	public List<Object> put(String key, List<Object> fields) {
		int i = indexOf(key);

		Row here = array[i];
		Row make = new Row(key, fields);

		//check that fields match degrees
		if(fields.size() != degree-1) {
			throw new IllegalArgumentException("Invalid degree size");
		}

		//if key already exists
		if (here != null) {
			array[i] = make;
			return here.fields();
		}
		//if key does not exist
		else
			array[i] = make;


		return null;
	}

	//  completed
	@Override
	public List<Object> get(String key) {
		int i = indexOf(key);
		Row here = array[i];

		if(here != null) {
			return here.fields();
		}

		return null;
	}

	// finished
	@Override
	public List<Object> remove(String key) {
		int i = indexOf(key);

		Row here = array[i];

		if (here != null) {
			array[i] = null;

			return here.fields();
		}

		return null;
	}

	// complete
	@Override
	public int degree() {
		return degree;
	}

	// complete
	@Override
	public int size() {
		int size = 0;
		for (Row row: array)
			if(row != null)
				size++;
		return size;
	}

	// complete
	@Override
	public int hashCode() {
		int fingerprint = 0;
		for (Row row: array)
			if (row != null)
				fingerprint += row.hashCode();
		return fingerprint;
	}

	@Override
	public boolean equals(Object obj) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Row> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		StringJoiner sj = new StringJoiner(", ", "LookupTable[", "]");
		sj.add("name" + name());
		sj.add("columns" + columns());
		for (Row row: array)
			if (row != null)
				sj.add(row.key() + "=" + row.fields());
		return sj.toString();
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public List<String> columns() {
		return columns;
	}
}