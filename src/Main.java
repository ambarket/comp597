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
		// HashMap<String, int[]> usersToReviews = new HashMap<String, int[]>();

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

		/*findKMostReviewsProducts(0,
				"C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\",
				"Amazon_Instant_Video.txt",
				"Amazon_Instant_Video_Most_Reviewed.csv");
		*/
		findKMostReviewsProductsWithReviews(0,
		"C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\",
		"Amazon_Instant_Video.txt",
		"Amazon_Instant_Video");
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
	}

	public static void findKMostReviewsProducts(int k, String basePath, String inputFile, String outputFile) throws IOException {
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
	
	
	public static void findKMostReviewsProductsWithReviews(int k, String basePath,String inputFile, String outputFilePrefix) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		HashMap<String, ProductCount> productCounts = getProductCounts(basePath, inputFile);
		HashMap<String, ReviewerCount> reviewerCounts = getReviewerCountsOnProducts(productCounts, basePath, inputFile);
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Read in counts of " + productCounts.size()
				+ " products in " + duration + " seconds.");

		startTime = System.nanoTime();

		outputProductCountsToCSV(productCounts, basePath, outputFilePrefix + "MostReviewedProducts.csv");
		outputReviewerCountsToCSV(reviewerCounts, basePath, outputFilePrefix + "MostCommonReviewersOfThoseProducts.csv");

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Wrote " + k + " most reviewed to file in "
				+ duration + " seconds.");
	}
	
	public static void outputReviewerCountsToCSV(HashMap<String, ReviewerCount> reviewerCounts, String basePath, String outputFile) {
		PriorityQueue<ReviewerCount> sortedReviewers = new PriorityQueue<ReviewerCount>();
		sortedReviewers.addAll(reviewerCounts.values());
		
		int k = reviewerCounts.size();
		CsvWriter csvOutput;
		try {
			csvOutput = new CsvWriter(new FileWriter(basePath + outputFile), ',');
			for (int i = 0; i < k; i++) {
				ReviewerCount rec = sortedReviewers.poll();
				csvOutput.write(rec.userID);
				csvOutput.write(String.valueOf(rec.reviewCount));
				csvOutput.endRecord();
			}
			csvOutput.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void outputProductCountsToCSV(HashMap<String, ProductCount> productCounts, String basePath, String outputFile) {
		PriorityQueue<ProductCount> sortedProducts = new PriorityQueue<ProductCount>();
		sortedProducts.addAll(productCounts.values());
		
		int k = productCounts.size();
		CsvWriter csvOutput;
		try {
			csvOutput = new CsvWriter(new FileWriter(basePath + outputFile), ',');
			for (int i = 0; i < k; i++) {
				ProductCount rec = sortedProducts.poll();
				csvOutput.write(rec.productID);
				csvOutput.write(String.valueOf(rec.reviewCount));
				csvOutput.endRecord();
			}
			csvOutput.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	
	public static HashMap<String, ReviewerCount> getReviewerCountsOnProducts(HashMap<String, ProductCount> products, String basePath, String inputFile) {
		HashMap<String, ReviewerCount> counts = new HashMap<String, ReviewerCount>();
		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));

			String line;
			Pattern startField = null;
			// includedFields =
			// Pattern.compile("^(product|review)/(productId:|title:|price:|userId:|profileName:|helpfulness:|score:|time:|summary:|text:)");
			startField = Pattern.compile("^product/productId:");
			Pattern non_printable = Pattern.compile("[\\x00\\x08\\x0B\\x0C\\x0E-\\x1F]|[\\\",#]");

			while ((line = br.readLine()) != null) {
				if (startField.matcher(line).find()) {
					String productID = line
							.replaceAll(startField.pattern(), "")
							.replaceAll(non_printable.pattern(), "").trim();
					// Skip title and price
					br.readLine();
					br.readLine();
					String userID = br.readLine()
							.replaceAll("^review/userId:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					
					// Skip if its unknown
					if (!userID.equals("unknown")) {
						if (products.containsKey(productID)) {
							if (!counts.containsKey(userID)) {
								counts.put(userID, new ReviewerCount(userID, 1));
							} else {
								counts.get(userID).reviewCount++;
							}
						}
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
					int Review = Integer.valueOf(record[7]
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
						records.put(productID, new ProductRecord(productID, productTitle,new Review(userID, Review, helpfulness)));
					} else {
						records.get(lineValue).reviews.add(new Review(userID,Review, helpfulness));
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



class ProductRecord implements Comparable<ProductRecord> {
	String productID;
	PriorityQueue<Review> reviews;

	public ProductRecord(String productID, String title, Review firstReview) {
		this.productID = productID;
		this.reviews = new PriorityQueue<Review>();
		this.reviews.add(firstReview);
	}

	public int compareTo(ProductRecord that) {
		return that.reviews.size() - this.reviews.size();
	}

	@Override
	public int hashCode() {
		return 37 * reviews.size() + productID.hashCode();
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

class ReviewerCount implements Comparable<ReviewerCount> {
	String userID;
	int reviewCount;

	public ReviewerCount(String userID, int reviewCount) {
		this.userID = userID;
		this.reviewCount = reviewCount;
	}

	public int compareTo(ReviewerCount that) {
		return that.reviewCount - this.reviewCount;
	}

	@Override
	public int hashCode() {
		return 37 * reviewCount + userID.hashCode();
	}

	@Override
	public boolean equals(Object that) {
		return userID.equals(((ReviewerCount) that).userID);
	}
}
