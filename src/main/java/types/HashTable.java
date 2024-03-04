package types;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import models.BoundedTable;
import models.Row;
import models.Table;


public class HashTable implements BoundedTable {


	private Row[] array;
	private String name;
	private List<String> columns;
	private int degree;
	private int size;
	private int capacity;
	private int fingerprint;
	private static int init_capacity = 19;
	private int contamination;
	private static final double LF_bound = 0.75;
	private static final Row TOMBSTONE = new Row(null,null);


	public HashTable(String name, List<String> columns) {
		this.name = name;
		this.columns = columns;
		this.degree = columns.size();
		clear();
	}

	//clear table- finished
	@Override
	public void clear() {
		this.capacity = init_capacity;
		array = new Row[capacity];
		this.size = 0;
		this.fingerprint =0;
		this.contamination =0;
	}
	private int hashFunction1(String key) {
		String salt = "gojo";
		int hash = 0;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(salt.getBytes());
			md.update(key.getBytes());

			byte[] digest = md.digest();
			BigInteger big_hash = new BigInteger(digest);
			hash = big_hash.intValue();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return Math.floorMod(hash, capacity);
	}

	//uses jenkins hash function
	private int hashFunction2(String key) {
		String salt = "gojo";
		String saltedKey = key+salt;
		int i = 1;
		int hash = 0;
		byte[] data = saltedKey.getBytes();
		while(i < saltedKey.length()) {
			hash += data[i++];
			hash += hash << 10;
			hash ^= hash >> 6;
		}
		hash += hash <<3;
		hash ^= hash >>11;
		hash += hash <<15;

		return Math.floorMod(hash, capacity()-1) + 1  ;
	}

	@Override
	public List<Object> put(String key, List<Object> fields) throws Exception {
		if(fields.size() != degree-1) {
			throw new IllegalArgumentException();
		}
		Row make = new Row(key, fields);
		int h = hashFunction1(key);
		int c = hashFunction2(key);
		int sentinal = -50;

		for(int j =0; j<capacity; j++) {
			int i = (h + j*c) % capacity;
			Row here = array[i];

			//set first tombstone index
			if(here == TOMBSTONE) {
				if(sentinal<0) {
					sentinal = i;
				}
				continue;
			}
			//miss
			if(array[i] == null) {
				if(sentinal > 0 ) {
					array[sentinal] = make;
					size++;
					contamination--;
					fingerprint += array[sentinal].hashCode();
				}else {
					array[i] = make;
					fingerprint += array[i].hashCode();
					size++;
				}
				if(this.loadFactor() >= LF_bound) {
					capacity = capacity*2;
					capacity++;
					while(!this.isPrime(capacity)) {
						capacity +=2;
					}
					this.rehash();
				}
				return null;
			}
			//hit
			if(here.key().equals(make.key())) {
				if(sentinal >0 ){
					array[i] = TOMBSTONE;
					array[sentinal] = make;
					fingerprint -= here.hashCode();
					fingerprint += make.hashCode();
				}else {
					fingerprint -= here.hashCode();
					fingerprint += make.hashCode();
					array[i] = make;
				}
				return here.fields();

			}

		}
		throw new Exception("Unexpected Fall-through");

	}

	@Override
	public List<Object> get(String key) throws Exception {
		int h = hashFunction1(key);
		int c = hashFunction2(key);
		for(int j =0; j<capacity;j++) {
			int i = (h+j*c) % capacity;
			Row here = array[i];
			if(here == TOMBSTONE) {
				continue;
			}
			if(here == null) {
				return null;
			}
			if(here.key().equals(key)) {
				return here.fields();
			}
		}
		throw new Exception("Unexpected Fall-through");

	}

	@Override
	public List<Object> remove(String key) {
		int h = hashFunction1(key);
		int c = hashFunction2(key);
		for(int j =0; j<capacity; j++) {
			int i = (h+j*c) % capacity;
			Row here = array[i];
			if(here == TOMBSTONE) {
				continue;
			}
			if(here == null) {
				return null;
			}
			if(here.key().equals(key)) {
				array[i] = TOMBSTONE;
				size--;
				contamination++;
				fingerprint -= here.hashCode();
				return here.fields();
			}
		}

		return null;		
	}

	private void rehash() throws Exception {
		Row[] backup = array.clone();
		array = new Row[capacity];
		size = 0;
		fingerprint = 0;
		contamination = 0;
		for(Row i : backup) {
			if(i == null || i == TOMBSTONE) {
				continue;
			}
			else 
				this.put(i.key(), i.fields());
		}
	}

	@Override
	public double loadFactor() {
		return (size+contamination) / capacity;
	}

	public boolean isPrime(int size) {
		if(size % 2 == 0) {
			return false;
		}
		for(int i =3; i<=Math.sqrt(size); i+=2) {
			if(size % i ==0) {
				return false;
			}
		}
		return true;
	}

	//returns degree- finished
	@Override
	public int degree() {
		return degree;
	}

	//returns size - finished
	@Override
	public int size() {
		return size;
	}

	//returns capacity - finished
	@Override
	public int capacity() {
		return capacity;
	}

	//returns the table hash code - finished
	@Override
	public int hashCode() {
		return fingerprint;
	}

	//returns if obj is equal to the table and is a Table
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

			int index =skip(0);
			@Override
			public boolean hasNext() {
				return index >= 0;
			}

			@Override
			public Row next() {
				// next of the Iterator (k3)
				//will only be called if hasNext was called correctly. throw exception if not
				if(hasNext()) {
					Row next = array[index];
					index = skip(index + 1);
					return next;

				}
				throw new NoSuchElementException();
			}


		};
	}

	public int skip(int index) {
		while(index < capacity && (array[index] == null || array[index].equals(TOMBSTONE))) {
			index++;
		}
		return index == capacity ? -1 : index;
	}


	//returns table name- finished
	@Override
	public String name() {
		return name;
	}

	//returns the columns of table - finished
	@Override
	public List<String> columns() {
		List<String> unmodList = this.columns;
		unmodList = Collections.unmodifiableList(unmodList);
		return unmodList;
	}

	@Override
	public String toString() {
		return this.toTabularView(null, false);
	}
}
