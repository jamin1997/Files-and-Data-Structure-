package models;

public interface BoundedTable extends Table {
	public int capacity();

	public default boolean isFull() {
		if(size() == capacity()) {
			return true;
		}
		return false;
	}

	public default double loadFactor() {
		return (double) size()/capacity();
	}
}
