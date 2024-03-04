package apps;

import java.util.Arrays;
import java.util.List;

import models.Row;
import models.Table;
import types.BinaryTable;


public class Sandbox {
	public static void main(String[] args) throws Exception {

		Table pets = new BinaryTable("Pets", List.of("Name", "Breed", "Age", "Color", "YearOfDeath", "Sex"));
		Row r = new Row("key", Arrays.asList("hello", "goodbye", 50, null));
		byte[] x = r.getBytes();
		System.out.println(Arrays.toString(x));
		System.out.println(Row.fromBytes(x).toString());

		//
		//		pets.put("Bischa", List.of("Portugese Waterdog", 6, "Black", 1999, "Male"));
		//		pets.put("Coco", List.of("Bichan Poodle", 16, "White", 2022, "Female"));
		//		pets.put("Tobi", Arrays.asList("Golden Doodle", 2, "Black", null, "Male"));
		//		pets.put("Pippen", List.of("Poodle", 15, "Black", 2023, "Male"));
		//		pets.put("Ginger", Arrays.asList("Golden Doodle", 7, "Gold", null, "Female"));
		//		pets.put("Tiger", Arrays.asList("Cat", 17, "Tabby", 1972, "Male"));
		//		System.out.println(pets.toString());

		//		Table pets = new XMLTable("Pets");
		//		pets.remove("Coco");
		//		pets.put("Clover", Arrays.asList("Golden Doodle", 4, "Golden", null, "Female"));
		//		pets.put("Zeke", Arrays.asList("Schnauzer", 13, "Gray", null, "Male"));
		//		System.out.println(pets.toString());
		//		Table games = new JSONTable("Games", List.of("Games", "Console", "AgeRestricted", "YearReleased"));
		//		games.put("COD: MW2", List.of("PC", true, 2022));
		//		games.put("Halo: Infinite", List.of("PC", true, 2021));
		//		games.put("Teamfight Tactics", List.of("Mobile", false, 2018));
		//		games.put("Horizon Forbidden West", List.of("PS5", true, 2022));
		//		System.out.println(games.toString());
		//		Table games = new JSONTable("Games");
		//		games.put("Baldurs Gate 3", List.of("PC", true, 2023));
		//		// these are different games with the same name in the same series
		//		games.put("COD: MW2", List.of("XBox", "true", 2009));
		//System.out.println(games.toString());


		//	m4_table1.toString();



	}
}
