import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;



public class Preprocessor {
	
	public static void createTabDelimitedFileFromRawDataset(String basePath, String inputFile, String outputFile, int captureBeginningOfReviewText) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to process raw dataset.");
		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(basePath + outputFile)));

			String line;
			Pattern startField = Pattern.compile("^product/productId:");
			Pattern non_printable = Pattern.compile("[\\x00\\x08\\x0B\\x0C\\x0E-\\x1F]|[\\\",#]");
			
			Pattern missing = Pattern.compile("^$|^unknown$");
			


			// Some reviews are duplicated in this dataset (same userId and productId), throw them
			//	in a HashSet to get rid of the dupes.
			HashSet<String> uniqueRatings = new HashSet<String>();
			
			while ((line = br.readLine()) != null) {
				if (startField.matcher(line).find()) {
					String productId = line
							.replaceAll(startField.pattern(), "")
							.replaceAll(non_printable.pattern(), "").trim();
					String productTitle = br.readLine()
							.replaceAll("^product/title:", "")
							.replaceAll(non_printable.pattern(), "")
							.trim();
					br.readLine(); // discard price
					String userId = br.readLine()
							.replaceAll("^review/userId:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					br.readLine(); // discard profileName
					br.readLine(); // discard helpfulness
					String rating = br.readLine()
							.replaceAll("^review/score:", "")
							.replaceAll(non_printable.pattern(), "").trim();
					String reviewText = "";
					if (captureBeginningOfReviewText != 0) {
						br.readLine();	// discard time
						br.readLine();	// discard summary
						reviewText = br.readLine()
								.replaceAll("^review/review:", "")
								.replaceAll(non_printable.pattern(), "").trim();
						reviewText = reviewText.substring(0, Math.min(reviewText.length(), captureBeginningOfReviewText));
					}
					
					String uniqueId = userId + "____" + productId;
					if (!uniqueRatings.contains(uniqueId) && !missing.matcher(userId).find()  && 
							!missing.matcher(productId).find() && !missing.matcher(rating).find() &&
							!missing.matcher(productTitle).find()) {
						uniqueRatings.add(uniqueId);
						
						bw.write(userId + '\t' + productId + '\t' + rating + '\t' + productTitle);
						if (reviewText.length() != 0) {
							bw.write('\t' + reviewText);
						}
						bw.write('\n');

						if (uniqueRatings.size() % 5000000 == 0) {
							bw.flush();
							System.out.println("Wrote " + uniqueRatings.size() + " ratings to tab delim file in " + watch.getElapsedSeconds() + " seconds");
						}
					}
				}
			}
			br.close();
			
			bw.flush();
			bw.close();
			
			System.out.println("Finished writing " + uniqueRatings.size() + " ratings to tab delim file in " + watch.getElapsedSeconds() + " seconds");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void createFileWithOnlyKRandomProducts(int k, String basePath, String inputFile, String outputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to minimize to " + k + " most reviewed products.");
		
		HashSet<DataCleanerProduct> products = readProductsFromFile(basePath, inputFile);
		
		ArrayList<DataCleanerProduct> sortedProductList = new ArrayList<DataCleanerProduct>(products);
		Collections.shuffle(sortedProductList);
		
		writeProductsToFile(sortedProductList.subList(0, k), basePath, outputFile);
		
		System.out.println("Finished minimizing " + products.size() + " products to " + k + " random products in " + watch.getElapsedSeconds() + " seconds");
	}

	public static void createFileWithOnlyKMostReviewedProducts(int k, String basePath, String inputFile, String outputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to minimize to " + k + " most reviewed products.");
		
		HashSet<DataCleanerProduct> products = readProductsFromFile(basePath, inputFile);
		
		ArrayList<DataCleanerProduct> sortedProductList = new ArrayList<DataCleanerProduct>(products);
		Collections.sort(sortedProductList);
		
		writeProductsToFile(sortedProductList.subList(0, k), basePath, outputFile);
		
		System.out.println("Finished minimizing " + products.size() + " products to " + k + " most reviewed products in " + watch.getElapsedSeconds() + " seconds");
	}
	
	// Note at this point we will lose the product titles, doesn't really matter though.
	public static void createFileWithOnlyKMostActiveUsers(int k, String basePath, String inputFile, String outputFile) {
		StopWatch watch = new StopWatch().start();
		System.out.println("Starting to minimize to " + k + " most active users.");
		
		HashSet<DataCleanerUser> users = readUsersFromFile(basePath, inputFile);
		
		ArrayList<DataCleanerUser> sortedUserList = new ArrayList<DataCleanerUser>(users);
		Collections.sort(sortedUserList);
		
		writeUsersToFile(sortedUserList.subList(0, k), basePath, outputFile);
		
		System.out.println("Finished minimizing " + users.size() + " users to " + k + " most active users in " + watch.getElapsedSeconds() + " seconds");
	}
	
	// Note at this point we will lose the product titles, doesn't really matter though.
	public static void createFileWithOnlyKMostReviewedProductsWithEachValue(int k, String basePath, String inputFile, String outputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to minimize to " + k + " most reviewed products.");
		
		 HashSet<DataCleanerProduct> products = readProductsFromFile(basePath, inputFile);
		 
		 ArrayList<DataCleanerProduct> sortedProductList = new ArrayList<DataCleanerProduct>(products);
		 Collections.sort(sortedProductList);
		 
		 ArrayList<DataCleanerProduct> selected = new ArrayList<DataCleanerProduct>();
		 int[] countRemainingForEachRating = { 0, k, k, k, k, k };

		 for (int rating = 1; rating < 6; rating++) {
			 for (DataCleanerProduct product : sortedProductList) {
				 int max = 0, ratingThatMax = 0;
				 for (int i = 0; i < 6; i++) {
					 if (product.ratingCounts[rating] > max) {
						 max = product.ratingCounts[rating];
						 ratingThatMax = rating;
					 }
				 }
				 if (countRemainingForEachRating[ratingThatMax] > 0) {
					 countRemainingForEachRating[ratingThatMax]--;
					 selected.add(product);
				 }
			 }
		 }
		 
		 System.out.println("selected: " + selected.size());
		 for (int i = 0; i < 6; i++) {
			 System.out.println("Count remaining for: " +  i + " - " + countRemainingForEachRating[i]);
		 }
	
		
		writeProductsToFile(selected, basePath, outputFile);
		
		System.out.println("Finished minimizing " + products.size() + " products to " + k + " most reviewed products in " + watch.getElapsedSeconds() + " seconds");
	}
	
	public static void integerizeUserAndProductIds(String basePath, String inputFile, String outputFile) {
		
		HashSet<Rating> ratings = readRatingsFromFile(basePath, inputFile);
		
		HashMap<String, Integer> productMap = mapProductIdsToIntegers(ratings);
		HashMap<String, Integer> userMap = mapUserIdsToIntegers(ratings);
		
		System.out.println("Products: " + productMap.keySet().size() + " Users: " + userMap.keySet().size());
		
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to write " + ratings.size() + " ratings to file with integer ids.");
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(basePath + outputFile)));
			
			for (Rating rating : ratings ) {
				bw.write(userMap.get(rating.userId).toString() + '\t' + productMap.get(rating.productId).toString() + '\t' + rating.ratingValue + '\n');
			}
			
			bw.flush();
			bw.close();
			
			bw = new BufferedWriter(new FileWriter(new File(basePath + outputFile + "-userIdMap.txt")));
			for (String userId : userMap.keySet() ) {
				bw.write(userId + "\t" + userMap.get(userId) + '\n');
			}
			
			bw.flush();
			bw.close();
			
			bw = new BufferedWriter(new FileWriter(new File(basePath + outputFile + "-productIdMap.txt")));
			for (String productId : productMap.keySet() ) {
				bw.write(productId + "\t" + productMap.get(productId) + '\n');
			}
			
			bw.flush();
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finished writing products to file in " + watch.getElapsedSeconds() + " seconds");
	}
	
	/*
	public static void removeSuspectedDuplicateProducts(String basePath, String inputFile, String outputFile) {
		HashSet<DataCleanerProduct> products = readProductsFromFile(basePath, inputFile);
		
		for (DataCleanerProduct product : products) {
			for (DataCleanerProduct product2 : products) {
				boolean 
			}
		}
		
		writeProductsToFile(sortedProductList.subList(0, k), basePath, outputFile);
	}
	*/
	
	public static HashSet<Rating> readRatingsFromFile(String basePath, String inputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to read ratings in from file.");
		
		HashSet<Rating> ratings = new HashSet<Rating>();
		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));

			String line;

			while ((line = br.readLine()) != null) {
				/*
				 * [0] - userId
				 * [1] - productId
				 * [2] - ratingValue
				 * [3] - title
				 */
				String[] components = line.split("\t");

				ratings.add(new Rating(components[0], components[1], (int)Double.parseDouble(components[2])));
			}
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		System.out.println("Finished reading " + ratings.size() + " ratings into memory in " + watch.getElapsedSeconds() + " seconds");
		
		return ratings;
	}
	
	public static HashSet<IntegerRating> readIntegerRatingsFromFile(String basePath, String inputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to read ratings in from file.");
		
		HashSet<IntegerRating> ratings = new HashSet<IntegerRating>();
		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));

			String line;

			while ((line = br.readLine()) != null) {
				/*
				 * [0] - userId
				 * [1] - productId
				 * [2] - ratingValue
				 * [3] - title
				 */
				String[] components = line.split("\t");

				ratings.add(new IntegerRating(Integer.parseInt(components[0]), Integer.parseInt(components[1]), Integer.parseInt(components[2])));
			}
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		System.out.println("Finished reading " + ratings.size() + " integer ratings into memory in " + watch.getElapsedSeconds() + " seconds");
		
		return ratings;
	}
	
	private static HashMap<String, Integer> mapProductIdsToIntegers(HashSet<Rating> ratings) {
		HashMap<String, Integer> retval = new HashMap<String, Integer>();
		int nextIntegerId = 0;
		for (Rating rating : ratings) {
			if (!retval.containsKey(rating.productId)) {
				retval.put(rating.productId, nextIntegerId);
				nextIntegerId++;
			}
		}
		return retval;
	}
	
	private static HashMap<String, Integer> mapUserIdsToIntegers(HashSet<Rating> ratings) {
		HashMap<String, Integer> retval = new HashMap<String, Integer>();
		int nextIntegerId = 0;
		for (Rating rating : ratings) {
			if (!retval.containsKey(rating.userId)) {
				retval.put(rating.userId, nextIntegerId);
				nextIntegerId++;
			}
		}
		return retval;
	}
	
	private static HashSet<DataCleanerProduct> readProductsFromFile(String basePath, String inputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to read products in from file.");
		
		HashMap<String, DataCleanerProduct> products = new HashMap<String, DataCleanerProduct>();
		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));

			String line;

			while ((line = br.readLine()) != null) {
				/*
				 * [0] - userId
				 * [1] - productId
				 * [2] - ratingValue
				 * [3] - title
				 */
				String[] components = line.split("\t");
				if (!products.containsKey(components[1])) {
					products.put(components[1], new DataCleanerProduct(components[1], components[3], false));
				}
				int ratingValue = (int)Double.parseDouble(components[2]);
				products.get(components[1]).ratingCounts[ratingValue]++;
				products.get(components[1]).ratings.add(new Rating(components[0], components[1], ratingValue));
			}
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		HashSet<DataCleanerProduct> retval = new HashSet<DataCleanerProduct>();
		retval.addAll(products.values());
		
		System.out.println("Finished reading " + products.size() + " products into memory in " + watch.getElapsedSeconds() + " seconds");
		return retval;
	}
	
	private static void writeProductsToFile(Collection<DataCleanerProduct> productList, String basePath, String outputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to write " + productList.size() + " products to file.");

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(basePath + outputFile)));
			
			for (DataCleanerProduct product : productList ) {
				for (Rating rating : product.ratings) {
					bw.write(rating.userId + '\t' + product.productId + '\t' + rating.ratingValue + '\t' + product.title + '\n');
				}

			}
			
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finished writing products to file in " + watch.getElapsedSeconds() + " seconds");
	}
	
	private static HashSet<DataCleanerUser> readUsersFromFile(String basePath, String inputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to read products in from file.");
		
		HashMap<String, DataCleanerUser> users = new HashMap<String, DataCleanerUser>();
		
		HashSet<String> uniqueProductIds = new HashSet<String>();
		HashSet<String> uniqueUserIds = new HashSet<String>();


		
		try {
			// use FileWriter constructor that specifies open for appending
			BufferedReader br = new BufferedReader(new FileReader(new File(basePath + inputFile)));

			String line;

			while ((line = br.readLine()) != null) {
				/*
				 * [0] - userId
				 * [1] - productId
				 * [2] - ratingValue
				 * [3] - title
				 */
				String[] components = line.split("\t");
				if (!users.containsKey(components[0])) {
					users.put(components[0], new DataCleanerUser(components[0]));
				}
				
				users.get(components[0]).ratings.add(new Rating(components[0], components[1], (int)Double.parseDouble(components[2])));
				
				uniqueProductIds.add(components[1]);
				uniqueUserIds.add(components[0]);
				
			}
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		HashSet<DataCleanerUser> retval = new HashSet<DataCleanerUser>();
		retval.addAll(users.values());

		System.out.println("Finished reading " + uniqueUserIds.size() + " users and " + uniqueProductIds.size() + " products into memory in " + watch.getElapsedSeconds() + " seconds");
		//System.out.println("Finished reading " + users.size() + " users into memory in " +  watch.getElapsedSeconds() + " seconds");
		return retval;
	}
	
	private static void writeUsersToFile(Collection<DataCleanerUser> userList, String basePath, String outputFile) {
		StopWatch watch = new StopWatch().start();
		
		System.out.println("Starting to write " + userList.size() + " users to file.");
		
		HashSet<String> uniqueProductIds = new HashSet<String>();
		HashSet<String> uniqueUserIds = new HashSet<String>();

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(basePath + outputFile)));
			
			for (DataCleanerUser user : userList ) {
				for (Rating rating : user.ratings) {
					bw.write(rating.userId + '\t' + rating.productId + '\t' + rating.ratingValue + '\t' + " " + '\n');
					
					uniqueProductIds.add(rating.productId);
					uniqueUserIds.add(rating.userId);
				}
			}
			
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		System.out.println("Finished writing " + uniqueUserIds.size() + " users and " + uniqueProductIds.size() + " products to file in " + watch.getElapsedSeconds() + " seconds");
	}
	
	private static class DataCleanerProduct implements Comparable<DataCleanerProduct>{
		String productId, title, cleanTitle;
		
		int ratingCounts[] = new int[6];
		
		HashSet<Rating> ratings;
		
		public DataCleanerProduct(String productId, String title, boolean cleanTitleFlag) {
			this.productId = productId;
			this.title = title;
			
			this.ratings = new HashSet<Rating>();
			
			if (cleanTitleFlag) {
				setCleanTitle();
			}
		}
		
		public boolean equals(Object obj)
		{
				if(this == obj) {
					return true;
				}
				
				if((obj == null) || (obj.getClass() != this.getClass())) {
					return false;
				}

				// object must be Rating at this point
				DataCleanerProduct test = (DataCleanerProduct)obj;
				return productId.equals(test.productId);
		}
		
		
		public int hashCode()
		{
			return productId.hashCode();
		}
		
		public int compareTo(DataCleanerProduct that) {
			return that.ratings.size() - this.ratings.size();
		}
		
		public void setCleanTitle() {
			cleanTitle = title.toLowerCase()
					.replaceAll("and|^the|of|or|edition|&|isbn|volume|[\\:]|[\\;]|[\\.]|[\\-]|[\\$]|[\\@]|[\\!]|[\\`]|[\\~]|[\\?]|'|,", "")
					.replaceAll("[ \t\n\r]+", "")
					.replaceAll("([\\(\\[].*[\\)\\]])+", "")
					.trim();
		}
		
		public boolean hasSimilarTitle(DataCleanerProduct other) {
			return cleanTitle.equals(other.cleanTitle) || (cleanTitle.contains(other.cleanTitle) || other.cleanTitle.contains(cleanTitle));
		}
	}
	
	private static class DataCleanerUser implements Comparable<DataCleanerUser>{
		String userId;
		HashSet<Rating> ratings;
		
		public DataCleanerUser(String userId) {
			this.userId = userId;	
			this.ratings = new HashSet<Rating>();
		}
		
		public boolean equals(Object obj)
		{
				if(this == obj) {
					return true;
				}
				
				if((obj == null) || (obj.getClass() != this.getClass())) {
					return false;
				}

				// object must be Rating at this point
				DataCleanerUser test = (DataCleanerUser)obj;
				return userId.equals(test.userId);
		}
		
		public int hashCode()
		{
			return userId.hashCode();
		}
		
		public int compareTo(DataCleanerUser that) {
			return that.ratings.size() - this.ratings.size();
		}
	}
}
