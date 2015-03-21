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
		 "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\Books\\", "Books.txt",
		 "Books");
		
		
		/*
		findKMostReviewsProductsWithReviews(100, 1000,
		"C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\Software\\",
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
		//----------------------------READ, SORT, PRUNE, and WRITE MOST REVIEWED PRODUCTS-----------------------------------------------
		
		long startTime, endTime, duration;
		startTime = System.nanoTime();

		HashMap<String, ProductCount> productCounts = getProductCounts(basePath, inputFile);
		
		ArrayList<ProductCount> sortedProductList = new ArrayList<ProductCount>(productCounts.values());
		Collections.sort(sortedProductList);
		int removed = 0;
		for (int i = 0; i < numOfProducts; i++) {
			for (int j = 1; j < 1000; j++) {
				if (i+j < sortedProductList.size()) {
					if (sortedProductList.get(i+j).equals(sortedProductList.get(i))) {
						sortedProductList.remove(i+j);
						removed++;
					}
				}
			}
		}
		productCounts = null;
		System.out.println("Removed " + removed + " duplicate titles");
		
		outputProductCountsToCSV(sortedProductList, basePath, "ALL-MostReviewedProducts" + "-"  + outputFilePrefix + ".csv");

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Read and saved sorted counts of " + sortedProductList.size() + " products in " + duration + " seconds.");
		
		
		sampleProductsAndDoTheRest(100, 1000, sortedProductList, basePath, inputFile, outputFilePrefix);
		sampleProductsAndDoTheRest(100, 10000, sortedProductList, basePath, inputFile, outputFilePrefix);
		sampleProductsAndDoTheRest(100, 50000, sortedProductList, basePath, inputFile, outputFilePrefix);
		
		sampleProductsAndDoTheRest(500, 1000, sortedProductList, basePath, inputFile, outputFilePrefix);
		sampleProductsAndDoTheRest(500, 10000, sortedProductList, basePath, inputFile, outputFilePrefix);
		sampleProductsAndDoTheRest(500, 50000, sortedProductList, basePath, inputFile, outputFilePrefix);
		
		sampleProductsAndDoTheRest(1000, 1000, sortedProductList, basePath, inputFile, outputFilePrefix);
		sampleProductsAndDoTheRest(1000, 10000, sortedProductList, basePath, inputFile, outputFilePrefix);
		sampleProductsAndDoTheRest(1000, 50000, sortedProductList, basePath, inputFile, outputFilePrefix);
		
		sampleProductsAndDoTheRest(Integer.MAX_VALUE, Integer.MAX_VALUE, sortedProductList, basePath, inputFile, outputFilePrefix);

	}
	
	public static void sampleProductsAndDoTheRest(int numOfProducts, int numOfReviewers, ArrayList<ProductCount> sortedProductList, String basePath, String inputFile, String outputFilePrefix) {
		long startTime, endTime, duration;
		//----------------------------SAMPLE MOST REVIEWED PRODUCTS-----------------------------------------------
		startTime = System.nanoTime();
		if (numOfProducts > sortedProductList.size()) {
			System.out.println("Only " + sortedProductList.size() + " products available, cannot sample " + numOfProducts +" will use all products instead");
			numOfProducts = sortedProductList.size();
		}
		ArrayList<ProductCount> sampledProductList = new ArrayList<ProductCount>(numOfProducts);

		for (int i = 0; i < numOfProducts; i++) {
			sampledProductList.add(sortedProductList.get(i));
		}
		sortedProductList = null;
		
		HashMap<String, ProductCount> sampledProductMap = new HashMap<String, ProductCount>(numOfProducts);
		for (int i = 0; i < numOfProducts; i++) {
			sampledProductMap.put(sampledProductList.get(i).productTitle, sampledProductList.get(i));
		}
		
		outputProductCountsToCSV(sampledProductList, basePath, numOfProducts + "-MostReviewedProducts" + "-"  + outputFilePrefix + ".csv");
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Sampled " + sampledProductList.size() + " most reviewed products in " + duration + " seconds.");
		
		//----------------------------READ, SORT, and WRITE MOST ACTIVE REVIEWERS OF THE SAMPLED PRODUCTS-----------------------------------------------
		startTime = System.nanoTime();
		HashMap<String, ReviewerCount> reviewerCounts = getReviewerCountsOnProducts(sampledProductMap, basePath, inputFile);
		ArrayList<ReviewerCount> sortedReviewerList = new ArrayList<ReviewerCount>(reviewerCounts.values());
		Collections.sort(sortedReviewerList);
		reviewerCounts = null;
		outputReviewerCountsToCSV(sortedReviewerList, basePath, "ALL-MostCommonReviewersOfThoseProducts"  + "-"  + outputFilePrefix + ".csv");
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Read and saved sorted counts of " + sortedReviewerList.size() + " reviewers of those products in " + duration + " seconds.");
		
		//----------------------------SAMPLE THOSE REVIEWERS-----------------------------------------------	
		startTime = System.nanoTime();
		if (numOfReviewers > sortedReviewerList.size()) {
			System.out.println("Only " + sortedReviewerList.size() + " reviewers available, cannot sample " + numOfReviewers +" will use all reviewers instead");
			numOfReviewers = sortedReviewerList.size();
		}
		ArrayList<ReviewerCount> sampledReviewerList = new ArrayList<ReviewerCount>(numOfReviewers);

		for (int i = 0; i < numOfReviewers; i++) {
			sampledReviewerList.add(sortedReviewerList.get(i));
		}
		
		sortedReviewerList = null;
		HashMap<String, ReviewerCount> sampledReviewerMap = new HashMap<String, ReviewerCount>(numOfReviewers);

		for (int i = 0; i < numOfReviewers; i++) {
			sampledReviewerMap.put(sampledReviewerList.get(i).userID, sampledReviewerList.get(i));
		}
		
		outputReviewerCountsToCSV(sampledReviewerList, basePath, numOfReviewers + "-MostCommonReviewersOfThoseProducts"   + "-"  + outputFilePrefix + ".csv");

		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Sampled " + sampledReviewerList.size() + " reviewers of those products in " + duration + " seconds.");

		//----------------------------READ, SORT, and WRITE DETAILED RECORDS OF REVIEWS OF THE SAMPLED PRODUCTS BY THE SAMPLED REVIEWERS-----------------------------------------------		
		startTime = System.nanoTime();
		HashMap<String, ProductRecord> productRecords = getProductRecords(sampledProductMap, sampledReviewerMap, basePath, inputFile);
		ArrayList<ProductRecord> sortedProductRecords = new ArrayList<ProductRecord>(productRecords.values());
		Collections.sort(sortedProductRecords);
		
		outputFinalCSV(sortedProductRecords, sampledReviewerList, basePath, numOfProducts + "P-" + numOfReviewers + "R" + "-"  + outputFilePrefix + ".csv");
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 / 1000;
		System.out.println("Retrieved " + productRecords.size() + " product records and output to final csv in " + duration + " seconds");
		/*
		System.out.println(productRecords.size());
		for (ProductRecord pr : productRecords.values()) {
			System.out.println(pr.toString());
		}
		*/
	}
	
	public static void outputFinalCSV(ArrayList<ProductRecord> sortedProductRecords, ArrayList<ReviewerCount>  sampledReviewerList, String basePath, String outputFilePrefix) {

		CsvWriter csvOutput;
		try {
			csvOutput = new CsvWriter(new FileWriter(basePath + "final" + "-" + outputFilePrefix + ".csv"), ',');
			// OutputHeader
			for (ProductRecord pc : sortedProductRecords) {
				csvOutput.write(pc.productTitle);
			}
			csvOutput.endRecord();
			//Output each record
			for (ReviewerCount rc : sampledReviewerList) {
				for (ProductRecord rec : sortedProductRecords) {
					//System.out.println(pc.productTitle + " " + rec);
					// Rec may be null if the reviewers who reviewed this product were not sampled.
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
	
	public static void outputReviewerCountsToCSV(ArrayList<ReviewerCount> sortedReviewers, String basePath, String outputFile) {
		CsvWriter csvOutput;
		try {
			csvOutput = new CsvWriter(new FileWriter(basePath + outputFile), ',');
			for (ReviewerCount rec : sortedReviewers) {
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
	
	public static void outputProductCountsToCSV(ArrayList<ProductCount> sortedProducts, String basePath, String outputFile) {
		CsvWriter csvOutput;
		try {
			csvOutput = new CsvWriter(new FileWriter(basePath + outputFile), ',');
			for (ProductCount rec : sortedProducts) {
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
