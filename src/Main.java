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
	   public csvGeneratorThread(String basePath, String inputFile, String outputFile, boolean withText) {
	       this.basePath = basePath;
	       this.inputFile = inputFile;
	       this.outputFile = outputFile;
	       this.withText = withText;
	   }

	   public void run() {
			long startTime, endTime, duration;
			startTime = System.nanoTime();
			

			
			/*
			String inputFile = "Amazon_Instant_Video.txt";
			String outputFile = "Amazon_Instant_Video_no_text2.csv";
			boolean withText = false;
			*/
			
			//List<String> titles = new LinkedList<String>();
			//List<String> userId = new LinkedList<String>();
			//String[] productIds = new String[22204];
			//HashMap<String, int[]> usersToRatings = new HashMap<String, int[]>();

			
			try {
				// use FileWriter constructor that specifies open for appending
				BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));
				CsvWriter csvOutput = new CsvWriter(new FileWriter(basePath + outputFile), ',');
				

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
					includedFields = Pattern.compile("^(product|review)/(productId:|title:|price:|userId:|profileName:|helpfulness:|score:|time:|summary:|text:)");
					textFields = Pattern.compile("^(product|review)/(productId:|title:|userId:|profileName:|helpfulness:|summary:|text:)");
					numberOfColumns = 11;
				}
				else {
					includedFields = Pattern.compile("^(product|review)/(productId:|title:|price:|userId:|profileName:|helpfulness:|score:|time:)");
					textFields = Pattern.compile("^(product|review)/(productId:|title:|userId:|profileName:|helpfulness:)");
					numberOfColumns = 9;
				}
				

				Pattern numericFields = Pattern.compile("^(product|review)/(price:|score:|time:)");
			    Pattern non_printable = Pattern.compile("[\\x00\\x08\\x0B\\x0C\\x0E-\\x1F]|[\\\",#]");
			    
				while((line=br.readLine())!=null){
					String lineValue = null;
					if (includedFields.matcher(line).find()) {
						lineValue = line.replaceAll(includedFields.pattern(), "").replaceAll(non_printable.pattern(), "").trim();
					}
					else {
						continue;
					}

					/* Should I really do this, or is it better to leave it as unknwon
					if (lineValue.equals("unknown")) {
						lineValue = "NA"; 
					}
					*/
					if (textFields.matcher(line).find()) {
						csvOutput.write("\"" + lineValue + "\"");
						colCount++;
						
						if (line.startsWith("review/helpfulness:")) {
							String[] helpfulness = lineValue.split("/");
							if (helpfulness.length == 2 && !helpfulness[1].equals("0"))
							{
								csvOutput.write(String.valueOf((Double.parseDouble(helpfulness[0]) / Double.parseDouble(helpfulness[1]))));
							}
							else {
								csvOutput.write("0");
							}
							colCount++;
						}
					}
					else if (numericFields.matcher(line).find()){
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
			duration = (endTime - startTime) / 1000000 /1000;
			System.out.println("Processed file in " + duration + " seconds."); 	
		}


}

public class Main {
	public static void main(String[] args) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();
		
		
		
		findKMostReviewsProducts(0, "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\", "Amazon_Instant_Video.txt", "Amazon_Instant_Video_Most_Reviewed.csv");
		//findKMostReviewsProducts(0, "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\", "Books.txt", "Books_Most_Reviewed.csv");
		//findKMostReviewsProducts(0, "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\", "Software.txt", "Software_Most_Reviewed.csv");
		
		/*
		csvGeneratorThread t = new csvGeneratorThread(basePath, inputFile, outputFile, withText);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		//List<String> titles = new LinkedList<String>();
		//List<String> userId = new LinkedList<String>();
		//String[] productIds = new String[22204];
		//HashMap<String, int[]> usersToRatings = new HashMap<String, int[]>();
	}

	public static void findKMostReviewsProducts(int k, String basePath, String inputFile, String outputFile) throws IOException {
		long startTime, endTime, duration;
		startTime = System.nanoTime();
		
		HashMap<String, ProductRecord> productCounts = getProductCounts(basePath, inputFile);
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 /1000;
		System.out.println("Read in counts of "  + productCounts.size() + " products in " + duration + " seconds."); 	
		
		startTime = System.nanoTime();
		
		PriorityQueue<ProductRecord> sortedProducts = new PriorityQueue<ProductRecord>();
		sortedProducts.addAll(productCounts.values());
		
		k = productCounts.size();
		CsvWriter csvOutput = new CsvWriter(new FileWriter(basePath + outputFile), ',');
		for (int i = 0; i < k; i++) {
			ProductRecord rec = sortedProducts.poll();
			csvOutput.write(rec.productID);
			csvOutput.write(String.valueOf(rec.reviewCount));
			csvOutput.endRecord();
		}
		
		csvOutput.close();
		
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000 /1000;
		System.out.println("Wrote " + k + " most reviewed to file in " + duration + " seconds."); 	
	}
	
	
	public static HashMap<String, ProductRecord> getProductCounts(String basePath, String inputFile) {
		HashMap<String, ProductRecord> counts = new HashMap<String, ProductRecord>();
		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));

			
			String line;
			Pattern includedFields = null;
			
			includedFields = Pattern.compile("^product/productId:");
			
			while((line=br.readLine())!=null){
				String lineValue = null;
				if (includedFields.matcher(line).find()) {
					lineValue = line.replaceAll(includedFields.pattern(), "").trim();
					if (!counts.containsKey(lineValue)) {
						counts.put(lineValue, new ProductRecord(lineValue, 1));
					}
					else {
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
	
}
	
class Rating implements Comparable<Rating> {
	String userID;
	int rating;
	
	@Override
	public int compareTo(Rating o) {
		return userID.compareTo(o.userID);
	}
}

class ProductRecord implements Comparable<ProductRecord> {
	String productID;
	int reviewCount;
	private PriorityQueue<Rating> ratings;
	public ProductRecord(String productID, int reviewCount) {
		this.productID = productID;
		this.reviewCount = reviewCount;
		this.ratings = new PriorityQueue<Rating>();
	}
	
	public int compareTo(ProductRecord that) {
		return that.reviewCount - this.reviewCount;
	}
	
	@Override
	public int hashCode() {
		return 37 * reviewCount  + productID.hashCode();
	}
	
	@Override
	public boolean equals(Object that) {
		return productID.equals(((ProductRecord)that).productID);
	}
}
