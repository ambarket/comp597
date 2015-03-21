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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.csvreader.CsvWriter;

// Maybe need something more like the netflix dataset
// column for each customer,row for each title



public class Main {
	public static void main(String[] args) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		/*
		findKMostReviewsProductsWithReviews(1000, 50000,
		"C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\",
		"Amazon_Instant_Video.txt",
		"Amazon_Instant_Video");
		*/
		
		
		findKMostReviewsProductsWithReviews(100, 1000,
		 "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\", "Books.txt",
		 "Books");
		
		
		/*
		findKMostReviewsProductsWithReviews(100, 1000,
		"C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\",
		"Software.txt", "Software");
		 */
		/*
		 * csvGeneratorThread t = new csvGeneratorThread(basePath, inputFile,
		 * outputFile, withText); t.start(); try { t.join(); } catch
		 * (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
	}
	
	
	public static void findKMostReviewsProductsWithReviews(int numOfProducts, int numOfReviewers, String basePath,String inputFile, String outputFilePrefix) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		HashMap<String, ProductCount> productCounts = getProductCounts(basePath, inputFile);
		HashMap<String, ReviewerCount> reviewerCounts = getReviewerCountsOnProducts(productCounts, basePath, inputFile);
		outputProductCountsToCSV(productCounts, basePath, outputFilePrefix + "MostReviewedProducts.csv");
		outputReviewerCountsToCSV(reviewerCounts, basePath, outputFilePrefix + "MostCommonReviewersOfThoseProducts.csv");
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Read and saved sorted counts of " + productCounts.size() + " products and " + reviewerCounts.size() + " reviewers of those products in " + duration + " seconds.");
		

		startTime = System.nanoTime();
		
		ArrayList<ProductCount> tmpProductList = new ArrayList<ProductCount>(productCounts.values());
		Collections.sort(tmpProductList);
		int removed = 0;
		for (int i = 0; i < numOfProducts; i++) {
			for (int j = 1; j < 500; j++) {
				if (i+j < tmpProductList.size()) {
					if (tmpProductList.get(i+j).equals(tmpProductList.get(i))) {
						tmpProductList.remove(i+j);
						removed++;
					}
				}
			}
		}
		System.out.println("Removed " + removed + " duplicate titles");
		
		ArrayList<ProductCount> sampledProductList = new ArrayList<ProductCount>(numOfProducts);
		for (int i = 0; i < numOfProducts; i++) {
			sampledProductList.add(tmpProductList.get(i));
		}
		productCounts = null;
		tmpProductList = null;
		
		ArrayList<ReviewerCount> tmpReviewerList = new ArrayList<ReviewerCount>(reviewerCounts.values());
		Collections.sort(tmpReviewerList);
		ArrayList<ReviewerCount> sampledReviewerList = new ArrayList<ReviewerCount>(numOfReviewers);
		for (int i = 0; i < numOfReviewers; i++) {
			sampledReviewerList.add(tmpReviewerList.get(i));
		}
		reviewerCounts = null;
		tmpReviewerList = null;
	
		
		HashMap<String, ProductCount> sampledProductMap = new HashMap<String, ProductCount>(numOfProducts);
		HashMap<String, ReviewerCount> sampledReviewerMap = new HashMap<String, ReviewerCount>(numOfReviewers);
		for (int i = 0; i < numOfProducts; i++) {
			sampledProductMap.put(sampledProductList.get(i).productTitle, sampledProductList.get(i));
		}
		for (int i = 0; i < numOfReviewers; i++) {
			sampledReviewerMap.put(sampledReviewerList.get(i).userID, sampledReviewerList.get(i));
		}

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Sorted and sampled " + sampledProductList.size() + " products and " + sampledReviewerList.size() + " reviewers of those products in " + duration + " seconds.");
		
		startTime = System.nanoTime();
		HashMap<String, ProductRecord> productRecords = getProductRecords(sampledProductMap, sampledReviewerMap, basePath, inputFile);
		outputFinalCSV(productRecords, sampledProductList, sampledReviewerList, basePath, outputFilePrefix);
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Retrieved product records and output to final csv in " + duration + " seconds");
		/*
		System.out.println(productRecords.size());
		for (ProductRecord pr : productRecords.values()) {
			System.out.println(pr.toString());
		}
		*/
	}
	
	public static void outputFinalCSV(HashMap<String, ProductRecord> productRecords, ArrayList<ProductCount> sampledProductList, ArrayList<ReviewerCount>  sampledReviewerList, String basePath, String outputFilePrefix) {

		CsvWriter csvOutput;
		try {
			csvOutput = new CsvWriter(new FileWriter(basePath + outputFilePrefix + "final.csv"), ',');
			// OutputHeader
			for (ProductCount pc : sampledProductList) {
				csvOutput.write(pc.productTitle);
			}
			csvOutput.endRecord();
			//Output each record
			for (ReviewerCount rc : sampledReviewerList) {
				for (ProductCount pc : sampledProductList) {
					ProductRecord rec = productRecords.get(pc.productTitle);
					if (rec.reviews.containsKey(rc.userID)) {
						csvOutput.write(rec.reviews.get(rc.userID).rating);
					}
					else {
						csvOutput.write("0");
					}
					
				}
				csvOutput.endRecord();
				
			}

			

			csvOutput.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
				csvOutput.write(rec.profileName);
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
				csvOutput.write(rec.productTitle);
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
					String productTitle = br.readLine()
							.replaceAll("^product/title:", "")
							.replaceAll(non_printable.pattern(), "")
							.trim();
					
					String cleanedProductTitle = productTitle
							.replaceAll("and|&|:|;", " ")
							.replaceAll("[ \t\n\r]+", " ")
							.toLowerCase().trim();
					
					if (!productTitle.equals("")) {
						if (!counts.containsKey(productTitle)) {
							counts.put(productTitle, new ProductCount(productID, productTitle, cleanedProductTitle, 1));
						} else {
							counts.get(productTitle).reviewCount++;
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
					String productTitle = br.readLine()
							.replaceAll("^product/title:", "")
							.replaceAll(non_printable.pattern(), "")
							.trim();
					// Skip price
					br.readLine();
					String userID = br.readLine()
							.replaceAll("^review/userId:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					String profileName = br.readLine()
							.replaceAll("^review/profileName:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					
					/*
					String cleanedProductTitle = productTitle
							.replaceAll("and|&|:|;", " ")
							.replaceAll("[ \t\n\r]+", " ")
							.toLowerCase().trim();
					*/
					// Skip if its unknown
					if (!userID.equals("unknown")) {
						if (products.containsKey(productTitle)) {
							if (!counts.containsKey(userID)) {
								counts.put(userID, new ReviewerCount(userID, profileName, 1));
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
	

	public static HashMap<String, ProductRecord> getProductRecords(HashMap<String, ProductCount> products, HashMap<String, ReviewerCount> reviewers, String basePath, String inputFile) {
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

			String[] record = new String[10];
			while ((line = br.readLine()) != null) {
				if (startField.matcher(line).find()) {
					record[0] = line;
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
							.replaceAll(non_printable.pattern(), "")
							.trim();
					String userID = record[3]
							.replaceAll("^review/userId:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					String rating = record[6]
							.replaceAll("^review/score:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					String helpfulness = record[5]
							.replaceAll("^review/helpfulness:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					
					
					String cleanedProductTitle = productTitle
							.replaceAll("and|&|:|;", " ")
							.replaceAll("[ \t\n\r]+", " ")
							.toLowerCase().trim();
					
					if (products.containsKey(productTitle) && reviewers.containsKey(userID)) {
						if (!records.containsKey(productTitle)) {
							records.put(productTitle, new ProductRecord(productID, productTitle, cleanedProductTitle, new Review(userID, rating, helpfulness)));
						} else {
							records.get(productTitle).reviews.put(userID, new Review(userID,rating, helpfulness));
						}
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
	String productTitle;
	String cleanTitle;
	HashMap<String, Review> reviews;

	public ProductRecord(String productID, String title, String cleanTitle, Review firstReview) {
		this.productID = productID;
		this.productTitle = title;
		this.cleanTitle = cleanTitle;
		this.reviews = new HashMap<String, Review>();
		this.reviews.put(firstReview.userID, firstReview);
	}

	public int compareTo(ProductRecord that) {
		return that.reviews.size() - this.reviews.size();
	}

	@Override
	public int hashCode() {
		return 37 * reviews.hashCode() + productTitle.hashCode() + productID.hashCode();
	}

	@Override
	public boolean equals(Object that) {
		System.out.println("In product record ==");
		return productTitle.equals(((ProductRecord) that).productTitle);

	}
	
	public String toString() {
		return this.productID + ": " + this.productTitle + " - " + this.cleanTitle + " : " + this.reviews.size() + " reviews";
	}
}

class ProductCount implements Comparable<ProductCount> {
	String productID;
	String productTitle;
	String cleanTitle;
	int reviewCount;

	public ProductCount(String productID, String productTitle, String cleanTitle, int reviewCount) {
		this.productID = productID;
		this.productTitle = productTitle;
		this.cleanTitle = cleanTitle;
		this.reviewCount = reviewCount;
	}

	public int compareTo(ProductCount that) {
		return that.reviewCount - this.reviewCount;
	}

	@Override
	public int hashCode() {
		return 37 * reviewCount + productID.hashCode() + productTitle.hashCode();
	}

	@Override
	public boolean equals(Object that) {
		//return productID.equals(((ProductRecord) that).productID);
		ProductCount other = (ProductCount) that;
		//return productTitle.equals(other.productTitle);
		return cleanTitle.contains(other.cleanTitle) || other.cleanTitle.contains(cleanTitle);
	}
}

class ReviewerCount implements Comparable<ReviewerCount> {
	String userID;
	String profileName;
	int reviewCount;

	public ReviewerCount(String userID, String profileName, int reviewCount) {
		this.userID = userID;
		this.profileName = profileName;
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
