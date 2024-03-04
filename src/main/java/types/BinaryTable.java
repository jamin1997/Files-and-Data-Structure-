package types;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import models.Row;
import models.StoredTable;
import models.Table;

public class BinaryTable implements StoredTable {

	private static final boolean zipFlag = true;
	private static final boolean byteFlag = false;
	private static Path root = Paths.get("db", "tables");
	private static FileSystem zipfs;
	private Path flat, data, metadata, vroot;

	public BinaryTable(String name, List<String> columns) {
		try {
			if(zipFlag) {
				flat = Paths.get("db", "tables", name+".zip");
				Files.createDirectories(flat.getParent());
				zipfs = FileSystems.newFileSystem(flat, Map.of("create", "true"));
				vroot = zipfs.getPath("/");

				data = vroot.resolve("data");
				Files.createDirectories(data);
				metadata = vroot.resolve("metadata");
				Files.createDirectories(metadata);

				Files.write(metadata.resolve("columns"), columns);

				flush();

			}
			else {
				flat = root.resolve(name);
				Files.createDirectories(flat);

				data = flat.resolve("data");
				metadata = flat.resolve("metadata");

				Files.createDirectories(data);
				Files.createDirectories(metadata);

				Files.write(metadata.resolve("columns"), columns);

				flush();
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public BinaryTable(String name) {
		if(zipFlag) {
			flat = Paths.get("db", "tables", name+".zip");
			try {
				Files.createDirectories(flat.getParent());
				if(Files.notExists(flat))
					throw new IllegalArgumentException();

				var zipfs = FileSystems.newFileSystem(flat, Map.of("create", "true"));
				var vroot = zipfs.getPath("/");

				data = vroot.resolve("data");
				Files.createDirectories(data);
				metadata = vroot.resolve("metadata");
				Files.createDirectories(metadata);


			} catch (IOException e) {
				throw new IllegalStateException();
			}
		}
		else{
			flat= root.resolve(name);
			if (Files.notExists(flat))
				throw new IllegalArgumentException("Missing Table: " + name);

			data = flat.resolve("data");
			metadata = flat.resolve("metadata");
		}
	}

	@Override
	public void clear() {
		try {
			Files.walk(data)
			.skip(1)
			.sorted(Comparator.reverseOrder())
			.forEach(path -> {
				try {
					Files.delete(path);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			});
			writeInt(metadata.resolve("size"), 0);
			writeInt(metadata.resolve("fingerprint"), 0);

		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static void writeInt(Path path, int i) {
		if (byteFlag) {
			var buffer = ByteBuffer.allocate(32);
			buffer.putInt(i);
			var array = buffer.array();

			try {
				Files.write(path,array);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} 
		else {
			try (var out = new ObjectOutputStream(Files.newOutputStream(path))) {
				out.writeInt(i);
				out.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static int readInt(Path path) {
		if (byteFlag) {
			try {
				var bytes = ByteBuffer.wrap(Files.readAllBytes(path));
				return bytes.getInt();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} 
		{
			try (var in = new ObjectInputStream(Files.newInputStream(path))) {

				var i = in.readInt();
				return i;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static void writeRow(Path path, Row row) {
		try {
			if(Files.notExists(path))
				Files.createDirectories(path.getParent());

			if (byteFlag) {
				var array = row.getBytes();
				Files.write(path, array);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (var out = new ObjectOutputStream(Files.newOutputStream(path))) 
		{
			out.writeObject(row);
			out.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Row readRow(Path path) {
		{
			try {
				if(byteFlag) {
					var bytes = Files.readAllBytes(path);
					return Row.fromBytes(bytes);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try (var in = new ObjectInputStream(Files.newInputStream(path))) 
			{
				var i = (Row) in.readObject();
				return i;
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private static void deleteRow(Path path) {
		try {
			Files.delete(path);
			if(path.getParent().getNameCount() == 0) {
				Files.delete(path.getParent());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String digestFunction(String key) {
		try {
			var sha1 = MessageDigest.getInstance("SHA-1");
			sha1.update("gojo".getBytes());
			sha1.update(key.getBytes());

			var digest = sha1.digest();
			var hex = HexFormat.of().withLowerCase();
			return hex.formatHex(digest);

		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	private Path pathOf(String digest) {
		var prefix = digest.substring(0, 2);
		var suffix = digest.substring(2);

		return data.resolve(prefix).resolve(suffix);
	}
	@Override
	public List<Object> put(String key, List<Object> fields) {
		if(fields.size() != this.degree()-1) {
			throw new IllegalArgumentException();
		}	
		var digest = digestFunction(key);
		var path = pathOf(digest);
		var row = new Row(key,fields);
		if(Files.exists(path)) {
			var read = readRow(path);
			writeRow(path, row);
			writeInt(metadata.resolve("fingerprint"), hashCode() - read.hashCode() + row.hashCode());
			return read.fields();
		}
		else {
			writeRow(path, row);
			writeInt(metadata.resolve("size"), size()+1);
			writeInt(metadata.resolve("fingerprint"), hashCode() + row.hashCode());
			return null;
		}
	}

	@Override
	public List<Object> get(String key) {
		var digest = digestFunction(key);
		var path = pathOf(digest);
		if(Files.exists(path)) {
			return readRow(path).fields();
		}
		return null;
	}

	@Override
	public List<Object> remove(String key) {
		var digest = digestFunction(key);
		var path = pathOf(digest);
		if(Files.exists(path)) {
			var old = readRow(path);
			deleteRow(path);
			writeInt(metadata.resolve("size"), size()-1);
			writeInt(metadata.resolve("fingerprint"), hashCode() - old.hashCode());
			return old.fields();
		}
		return null;
	}

	@Override
	public int degree() {
		return columns().size();
	}

	@Override
	public int size() {
		return readInt(metadata.resolve("size"));
	}

	@Override
	public int hashCode() {
		return readInt(metadata.resolve("fingerprint"));
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Table && obj.hashCode() == this.hashCode())
			return true;
		return false;
	}

	@Override
	public void flush() {
		if(zipFlag) {
			try {
				zipfs.close();
				zipfs = FileSystems.newFileSystem(flat);
				vroot = zipfs.getPath("/");
				data = vroot.resolve("data");
				metadata = vroot.resolve("metadata");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public Iterator<Row> iterator() {
		if(zipFlag) {
			flush();
		}

		try {
			return Files.walk(data)
					.filter(file-> !Files.isDirectory(file))
					.map(path -> readRow(path))
					.iterator();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String name() {
		if(zipFlag) {
			int dotIndex = flat.getFileName().toString().lastIndexOf('.');
			return flat.getFileName().toString().substring(0, dotIndex);
		}
		else {
			return flat.getFileName().toString();
		}
	}

	@Override
	public List<String> columns() {
		try {
			return Files.readAllLines(metadata.resolve("columns"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}