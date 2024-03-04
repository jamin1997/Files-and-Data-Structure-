package models;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import types.HashTable;

public interface Table extends Iterable<Row> {
	public void clear() throws IOException;

	public List<Object> put(String key, List<Object> fields) throws Exception;

	public List<Object> get(String key) throws Exception;

	public List<Object> remove(String key) throws Exception;

	public default boolean contains(String key) throws Exception {
		if(get(key) != null) {
			return true;
		}
		return false;
	}

	public int degree();

	public int size();

	public default boolean isEmpty() {
		if(size() == 0) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode();

	@Override
	public boolean equals(Object obj);


	@Override
	public Iterator<Row> iterator();

	public String name();

	public List<String> columns();

	public default Table filter(Object search) throws Exception {
		if(search == null) {
			throw new IllegalArgumentException();
		}
		String name = this.name()+"_partition";
		Table partition = new HashTable(name, this.columns());
		for(Row here : this) {
			if(here.key().equals(search)) {
				partition.put(here.key(), here.fields());
			}

			for(int i=0; i<here.fields().size(); i++) {
				var item = here.fields().get(i);

				//skips past nulls in the middle of fields
				while(item == null) {
					if(i == here.fields().size()-1) 
						break;
					i++;
					item = here.fields().get(i);
				}
				//skips last field if null
				if(item == null) {
					break;
				}

				if(item.equals(search) || search.equals(item.toString())) {
					partition.put(here.key(), here.fields());
				}
			}
		}
		return partition;
	}

	@Override
	public String toString();

	public default String toTabularView(List<Row> array, boolean sorted) {

		List<Row> sortedList = new ArrayList<Row>();
		for(Row i : array) {
			sortedList.add(i);
		}

		//if sorted = true, sort list
		if(sorted) {
			Collections.sort(sortedList);
		}

		StringBuilder sb = new StringBuilder();
		//add table name
		sb.append(this.name());
		sb.append("\n");
		//create header box
		//sb.append(sortedList.get(1).fields().get(1));
		for(int i = 0; i<this.columns().size();i++) {
			sb.append("+---------------");
		}

		sb.append("+");
		sb.append("\n");
		sb.append("|");
		for(int i =0; i <this.columns().size(); i++) {
			String word = this.columns().get(i).toString();
			if(word.length()>15) {
				sb.append("%-15s".formatted(tooLong(word)));
			}
			else
				sb.append("%-15s".formatted(this.columns().get(i)));
			sb.append("|");
		}
		sb.append("\n");
		for(int i = 0; i<this.columns().size();i++) {
			sb.append("+---------------");
		}
		sb.append("+");
		sb.append("\n");

		//add keys
		for(Row i: sortedList) {
			sb.append("|");
			String word = i.key().toString();
			if(word.length()>15) {
				sb.append("%-15s".formatted(tooLong(word)));
			}
			else {
				sb.append("%-15s".formatted(i.key()));
			}
			sb.append("|");
			//add fields
			for(int j =0;j<i.fields().size();j++)
			{
				//check for alignment
				if(i.fields().get(j) instanceof Number) {
					sb.append("%15s".formatted(i.fields().get(j)));
					sb.append("|");
				}
				else if(i.fields().get(j) == null){
					sb.append("%15s".formatted(""));
					sb.append("|");
				}
				else {
					word = i.fields().get(j).toString();
					if(word.length()>15) {
						sb.append("%-15s".formatted(tooLong(word)));
					}
					else {
						sb.append("%-15s".formatted(i.fields().get(j)));
					}

					sb.append("|");
				}

			}
			sb.append("\n");
		}

		for(int i = 0; i<this.columns().size();i++) {
			sb.append("+---------------");
		}
		sb.append("+");
		//System.out.println(sb);
		return sb.toString();
	}

	public default Table union(Table that) throws Exception {
		if(this.degree() != that.degree()) {
			throw new IllegalArgumentException();
		}
		for(Row i : that) {
			this.put(i.key(), i.fields());
		}
		return this;
	}

	public default Table intersect(Table that) throws Exception{
		if(this.degree() != that.degree()) {
			throw new IllegalArgumentException();
		}
		List<Row> removed = new ArrayList<Row>();
		for(Row i : this) {
			if(!that.contains(i.key())) {
				removed.add(i);
			}
		}
		for(Row j : removed) {
			this.remove(j.key());
		}
		return this;
	}

	public default Table minus(Table that) throws Exception {
		if(this.degree() != that.degree()) {
			throw new IllegalArgumentException();
		}
		List<Row> removed = new ArrayList<Row>();
		for(Row i : this) {
			if(that.contains(i.key())) {
				removed.add(i);
			}
		}
		for(Row j : removed) {
			this.remove(j.key());
		}
		return this;
	}

	public default Table keep(Object obj) throws Exception {
		this.intersect(this.filter(obj));
		return this;
	}

	public default Table drop(Object obj) throws Exception {
		this.minus(this.filter(obj));
		return this;
	}

	public default String tooLong(String word) {
		String dots = "...";
		String shorter = word.substring(0,12) + dots;
		return shorter;

	}

}
