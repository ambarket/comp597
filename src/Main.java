/*
 * amazon <- read.csv("output.csv")
 * str(books)
 * table(count.fields("output.csv", sep=",", quote=""))
 * txt <-readLines("output.csv")[which(count.fields("output.csv", quote="", sep=",") == 19)]
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.csvreader.CsvWriter;

// Maybe need something more like the netflix dataset
// column for each customer,row for each title

class csvGeneratorThread extends Thread {
	private String basePath;
	private String inputFile;
	private String outputFile;
	private boolean withText;

	public csvGeneratorThread(String basePath, String inputFile,
			String outputFile, boolean withText) {
		this.basePath = basePath;
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.withText = withText;
	}

	public void run() {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		/*
		 * String inputFile = "Amazon_Instant_Video.txt"; String outputFile =
		 * "Amazon_Instant_Video_no_text2.csv"; boolean withText = false;
		 */

		// List<String> titles = new LinkedList<String>();
		// List<String> userId = new LinkedList<String>();
		// String[] productIds = new String[22204];
		// HashMap<String, int[]> usersToRatings = new HashMap<String, int[]>();

		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(
					basePath + inputFile)));
			CsvWriter csvOutput = new CsvWriter(new FileWriter(basePath
					+ outputFile), ',');

			// Write the header
			csvOutput.write("product.productId");
			csvOutput.write("product.title");
			csvOutput.write("product.price");
			csvOutput.write("review.userId");
			csvOutput.write("review.profileName");
			csvOutput.write("review.helpfulness_orig");
			csvOutput.write("review/helpfulness");
			csvOutput.write("review.score");
			csvOutput.write("review.time");
			if (withText) {
				csvOutput.write("review.summary");
				csvOutput.write("review.text");
			}
			csvOutput.endRecord();

			String line;
			int colCount = 0;
			Pattern includedFields = null;
			Pattern textFields = null;
			int numberOfColumns = 0;
			if (withText) {
				includedFields = Pattern
						.compile("^(product|review)/(productId:|title:|price:|userId:|profileName:|helpfulness:|score:|time:|summary:|text:)");
				textFields = Pattern
						.compile("^(product|review)/(productId:|title:|userId:|profileName:|helpfulness:|summary:|text:)");
				numberOfColumns = 11;
			} else {
				includedFields = Pattern
						.compile("^(product|review)/(productId:|title:|price:|userId:|profileName:|helpfulness:|score:|time:)");
				textFields = Pattern
						.compile("^(product|review)/(productId:|title:|userId:|profileName:|helpfulness:)");
				numberOfColumns = 9;
			}

			Pattern numericFields = Pattern
					.compile("^(product|review)/(price:|score:|time:)");
			Pattern non_printable = Pattern
					.compile("[\\x00\\x08\\x0B\\x0C\\x0E-\\x1F]|[\\\",#]");

			while ((line = br.readLine()) != null) {
				String lineValue = null;
				if (includedFields.matcher(line).find()) {
					lineValue = line.replaceAll(includedFields.pattern(), "")
							.replaceAll(non_printable.pattern(), "").trim();
				} else {
					continue;
				}

				/*
				 * Should I really do this, or is it better to leave it as
				 * unknwon if (lineValue.equals("unknown")) { lineValue = "NA";
				 * }
				 */
				if (textFields.matcher(line).find()) {
					csvOutput.write("\"" + lineValue + "\"");
					colCount++;

					if (line.startsWith("review/helpfulness:")) {
						String[] helpfulness = lineValue.split("/");
						if (helpfulness.length == 2
								&& !helpfulness[1].equals("0")) {
							csvOutput.write(String.valueOf((Double
									.parseDouble(helpfulness[0]) / Double
									.parseDouble(helpfulness[1]))));
						} else {
							csvOutput.write("0");
						}
						colCount++;
					}
				} else if (numericFields.matcher(line).find()) {
					csvOutput.write(lineValue);
					colCount++;
				}
				if (colCount == numberOfColumns) {
					csvOutput.endRecord();
					colCount = 0;
				}
			}
			csvOutput.close();
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Processed file in " + duration + " seconds.");
	}

}

public class Main {
	public static void main(String[] args) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		findKMostReviewsProducts(0,
				"C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\",
				"Amazon_Instant_Video.txt",
				"Amazon_Instant_Video_Most_Reviewed.csv");
		// findKMostReviewsProducts(0,
		// "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\", "Books.txt",
		// "Books_Most_Reviewed.csv");
		// findKMostReviewsProducts(0,
		// "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\",
		// "Software.txt", "Software_Most_Reviewed.csv");

		/*
		 * csvGeneratorThread t = new csvGeneratorThread(basePath, inputFile,
		 * outputFile, withText); t.start(); try { t.join(); } catch
		 * (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */

		// List<String> titles = new LinkedList<String>();
		// List<String> userId = new LinkedList<String>();
		// String[] productIds = new String[22204];
		// HashMap<String, int[]> usersToRatings = new HashMap<String, int[]>();
	}

	public static void findKMostReviewsProducts(int k, String basePath,
			String inputFile, String outputFile) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		HashMap<String, ProductCount> productCounts = getProductCounts(
				basePath, inputFile);

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Read in counts of " + productCounts.size()
				+ " products in " + duration + " seconds.");

		startTime = System.nanoTime();

		PriorityQueue<ProductCount> sortedProducts = new PriorityQueue<ProductCount>();
		sortedProducts.addAll(productCounts.values());

		k = productCounts.size();
		CsvWriter csvOutput = new CsvWriter(new FileWriter(basePath
				+ outputFile), ',');
		for (int i = 0; i < k; i++) {
			ProductCount rec = sortedProducts.poll();
			csvOutput.write(rec.productID);
			csvOutput.write(String.valueOf(rec.reviewCount));
			csvOutput.endRecord();
		}

		csvOutput.close();

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Wrote " + k + " most reviewed to file in "
				+ duration + " seconds.");
	}
	
	public static void findKMostReviewsProductsWithRatings(int k, String basePath,String inputFile, String outputFile) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		HashMap<String, ProductRecord> productCounts = getProductRecords(basePath, inputFile);

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Read in counts of " + productCounts.size()
				+ " products in " + duration + " seconds.");

		startTime = System.nanoTime();

		PriorityQueue<ProductRecord> sortedProducts = new PriorityQueue<ProductRecord>();
		sortedProducts.addAll(productCounts.values());

		k = productCounts.size();
		CsvWriter csvOutput = new CsvWriter(new FileWriter(basePath
				+ outputFile), ',');
		for (int i = 0; i < k; i++) {
			ProductRecord rec = sortedProducts.poll();
			csvOutput.write(rec.productID);
			int numOfRatings =  rec.ratings.size();
			for (int j = 0; j < numOfRatings; j--) {
				csvOutput.write(String.valueOf(rec.reviewCount));
			}
			csvOutput.write(String.valueOf(rec.reviewCount));
			csvOutput.endRecord();
		}

		csvOutput.close();

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Wrote " + k + " most reviewed to file in "
				+ duration + " seconds.");
	}

	public static HashMap<String, ProductCount> getProductCounts(
			String basePath, String inputFile) {
		HashMap<String, ProductCount> counts = new HashMap<String, ProductCount>();

		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(
					basePath + inputFile)));

			String line;
			Pattern includedFields = null;

			includedFields = Pattern.compile("^product/productId:");

			while ((line = br.readLine()) != null) {
				String lineValue = null;
				if (includedFields.matcher(line).find()) {
					lineValue = line.replaceAll(includedFields.pattern(), "")
							.trim();
					if (!counts.containsKey(lineValue)) {
						counts.put(lineValue, new ProductCount(lineValue, 1));
					} else {
						counts.get(lineValue).reviewCount += 1;
					}
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return counts;
	}

	public static HashMap<String, ProductRecord> getProductRecords(
			String basePath, String inputFile) {
		HashMap<String, ProductRecord> records = new HashMap<String, ProductRecord>();

		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(
					basePath + inputFile)));

			String line;
			Pattern startField = null;
			// includedFields =
			// Pattern.compile("^(product|review)/(productId:|title:|price:|userId:|profileName:|helpfulness:|score:|time:|summary:|text:)");
			startField = Pattern.compile("^product/productId:");
			Pattern non_printable = Pattern
					.compile("[\\x00\\x08\\x0B\\x0C\\x0E-\\x1F]|[\\\",#]");

			int unknownCount = 0;
			while ((line = br.readLine()) != null) {
				String lineValue = null;
				if (startField.matcher(line).find()) {
					String[] record = new String[10];
					record[0] = lineValue;
					for (int i = 1; i < 10; i++) {
						line = br.readLine();
						if (line == null) {
							System.out.println("SOmething terrible happened");
						} else {
							record[i] = line;
						}
					}

					String productID = record[0]
							.replaceAll(startField.pattern(), "")
							.replaceAll(non_printable.pattern(), "").trim();
					String productTitle = record[1]
							.replaceAll("^product/title:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					String userID = record[3]
							.replaceAll("^product/userId:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					int rating = Integer.valueOf(record[7]
							.replaceAll("^product/score:", "")
							.replaceAll(non_printable.pattern(), "").trim());
					String helpfulness = record[8]
							.replaceAll("^product/helpfulness:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					
					if (userID.equals("unknown")) {
						userID = "unknown" + unknownCount;
						unknownCount++;
					}
					
					if (!records.containsKey(productID)) {
						records.put(productID, new ProductRecord(productID, productTitle,new Rating(userID, rating, helpfulness)));
					} else {
						records.get(lineValue).ratings.add(new Rating(userID,rating, helpfulness));
					}
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return records;
	}
}

class Rating implements Comparable<Rating> {
	String userID;
	int rating;
	String helpfulness;

	public Rating(String userID, int rating, String helpfulness) {
		this.userID = userID;
		this.rating = rating;
		this.helpfulness = helpfulness;
	}

	@Override
	public int compareTo(Rating o) {
		return userID.compareTo(o.userID);
	}
}

class ProductRecord implements Comparable<ProductRecord> {
	String productID;
	PriorityQueue<Rating> ratings;

	public ProductRecord(String productID, String title, Rating firstRating) {
		this.productID = productID;
		this.ratings = new PriorityQueue<Rating>();
		this.ratings.add(firstRating);
	}

	public int compareTo(ProductRecord that) {
		return that.ratings.size() - this.ratings.size();
	}

	@Override
	public int hashCode() {
		return 37 * ratings.size() + productID.hashCode();
	}

	@Override
	public boolean equals(Object that) {
		return productID.equals(((ProductRecord) that).productID);
	}
}

class ProductCount implements Comparable<ProductCount> {
	String productID;
	int reviewCount;

	public ProductCount(String productID, int reviewCount) {
		this.productID = productID;
		this.reviewCount = reviewCount;
	}

	public int compareTo(ProductCount that) {
		return that.reviewCount - this.reviewCount;
	}

	@Override
	public int hashCode() {
		return 37 * reviewCount + productID.hashCode();
	}

	@Override
	public boolean equals(Object that) {
		return productID.equals(((ProductRecord) that).productID);
	}
}
