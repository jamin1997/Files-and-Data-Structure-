package types;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import models.BoundedTable;
import models.Row;
import models.Table;

public class SearchTable implements BoundedTable {

	private Row[] array;
	private String name;
	private List<String> columns;
	private int degree;
	private int size;
	private int capacity;
	private int fingerprint;
	private static int init_capacity = 16;

	//potentially completed
	public SearchTable(String name, List<String> columns) {
		this.name = name;
		this.columns = columns;
		this.degree = columns.size();
		clear();
	}

	//in progress - potentially complete
	@Override
	public void clear() {
		this.capacity = init_capacity;
		this.fingerprint =0;
		array = new Row[capacity];
		this.size = 0;
	}

	@Override
	public List<Object> put(String key, List<Object> fields) {
		if(fields.size() != degree-1) {
			throw new IllegalArgumentException();
		}

		Row make = new Row(key, fields);
		int index;
		//check if key already exists in Table, replaces if true
		for(index=0; index<size();index++) {
			Row here = array[index];
			if(here.key().equals(make.key())) {
				fingerprint -= here.hashCode();
				fingerprint += make.hashCode();
				array[index] = make;
				return here.fields();
			}
		}
		//if row does not already exist, add
		array[index] = make;
		fingerprint += array[index].hashCode(); //adjust fingerprint
		this.size++;	//increment size

		//check if array is full, adjust capacity if so
		if(size == capacity) {
			this.capacity *= 2;
			Row[] arrayCopy = Arrays.copyOf(array, capacity());
			array = arrayCopy;
		}


		return null;
	}


	@Override
	public List<Object> get(String key) {
		for(int index=0; index<size;index++) {
			Row here = array[index];
			if(here.key().equals(key)) {
				return here.fields();
			}
		}
		return null;
	}

	@Override
	public List<Object> remove(String key) {
		//check if key is in table
		for(int index=0; index<size();index++) {
			Row here = array[index];

			//if key matches existing row key, remove key, decrement size.
			if(here.key().equals(key)) {
				array[index] = array[size()-1];
				array[size()-1] = null;
				this.size--;
				fingerprint -= here.hashCode();
				return here.fields();
			}
		}

		return null;
	}

	//complete
	@Override
	public int degree() {
		return degree;
	}

	//potentially complete
	@Override
	public int size() {
		return size;
	}

	//complete
	@Override
	public int capacity() {
		return capacity;
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj.hashCode() == this.hashCode() && obj instanceof Table) {
			return true;
		}
		return false;
	}

	@Override
	public Iterator<Row> iterator() {

		return new Iterator<Row>() {
			int index =0;
			@Override
			public boolean hasNext() {
				//hasNext of iterator(K2)
				if(index < size()) {
					return true;
				}
				return false;
			}

			@Override
			public Row next() {
				// next of the Iterator (k3)
				//will only be called if hasNext was called correctly. throw exception if not
				if(hasNext()) {
					var temp = array[index];
					index++;
					return temp;
				}
				throw new NoSuchElementException();
			}

		};
	}

	//complete
	@Override
	public String name() {
		return name;
	}

	//complete
	@Override
	public List<String> columns() {
		List<String> unmodList = this.columns;
		unmodList = Collections.unmodifiableList(unmodList);
		return columns;
	}

	@Override
	public String toString() {
		return this.array.toString();

	}
}