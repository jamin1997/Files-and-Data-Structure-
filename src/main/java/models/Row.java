package models;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record Row(String key, List<Object> fields) implements Comparable<Row>, Serializable{

	public Row {
		if(fields != null) {
			fields = Collections.unmodifiableList(new ArrayList<Object>(fields));
		}
	}
	@Override
	public String toString() {
		return  key + "=" + fields;
	}

	@Override
	public int hashCode() {
		return key.hashCode() * 31 + fields.hashCode();
	}

	public static int hashCode(String key, List<Object> fields) {
		return key.hashCode() * 31 + fields.hashCode();

	}

	@Override
	public int compareTo(Row that) {
		return this.key.compareTo(that.key);
	}

	public byte[] getBytes() {
		List<Object> rows = new ArrayList<Object>();
		rows.add(key);
		for(Object i : fields)
			rows.add(i);
		int size = 0;
		for(Object j : rows) {
			if(j instanceof String) {
				size += ((String)j).length() + 1;
			}
			if(j instanceof Integer) {
				size += 5;
			}
			if(j instanceof Double) {
				size += 9;
			}
			if(j instanceof Boolean) {
				size += 1;
			}
			if(j == null) {
				size += 1;
			}
		}
		var buffer = ByteBuffer.allocate(size);
		for(Object k : rows) {
			if(k == null) {
				buffer.put((byte) -20);
			}
			else if(k instanceof String) {
				buffer.put((byte) k.toString().length());
				buffer.put(k.toString().getBytes());
			}
			else if(k instanceof Integer) {
				buffer.put((byte) -1);
				buffer.putInt((int)k);
			}
			else if(k instanceof Double) {
				buffer.put((byte) -2);
				buffer.putDouble((double) k);
			}
			else if(k.equals(true)) {
				buffer.put((byte) -10);
			}
			else if(k.equals(false)) {
				buffer.put((byte) -15);
			}

		}
		var output = buffer.array();
		//System.out.println(Arrays.toString(output));
		return output;
	}

	public static Row fromBytes(byte[] bytes) {
		List<Object> row = new ArrayList<Object>();
		var buffer = ByteBuffer.wrap(bytes);
		while(buffer.hasRemaining()) {
			var get = buffer.get();
			if(get == -1) {
				row.add(buffer.getInt());
			}
			else if(get == -2) {
				row.add(buffer.getDouble());
			}
			else if(get == -10) {
				row.add(true);
			}
			else if(get == -15) {
				row.add(false);
			}
			else if(get == -20)
				row.add(null);
			else if(get >0) {
				var array = new byte[get];
				buffer.get(array);
				var string = new String(array);
				row.add(string);
			}

		}
		var x = new Row((String) row.get(0), row.subList(1, row.size()));
		return x;
	}



}