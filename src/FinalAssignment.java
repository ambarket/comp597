
import java.util.ArrayList;
import java.util.HashSet;

 
public class FinalAssignment {

	static String BASE_PATH = "C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset\\Books\\";
	
	static int NUMBER_OF_USERS = 10000;
	static int NUMBER_OF_PRODUCTS = 1000;
	
	static String MOST_REVIEWED = "Books-" + NUMBER_OF_PRODUCTS + "-most-reviewed.txt";
	
	static String MOST_REVIEWED_AND_MOST_ACTIVE = "Books-" + NUMBER_OF_PRODUCTS + "-most-reviewed-" + NUMBER_OF_USERS + "-most-active-users.txt";
	
	static String MOST_REVIEWED_AND_MOST_ACTIVE_WITH_INTEGER_IDS = "Books-" + NUMBER_OF_PRODUCTS + "-most-reviewed-" + NUMBER_OF_USERS + "-most-active-users-Integer-IDS.txt";

	//static ArrayList<Rating> allRatings;
	public static void main(String[] args) {
		StopWatch watch = new StopWatch().start();
		
		//Preprocessor.createTabDelimitedFileFromRawDataset(BASE_PATH, "Books.txt", "Books-tab-delim-80charReview.txt", 80);
		Preprocessor.createFileWithOnlyKMostReviewedProducts(NUMBER_OF_PRODUCTS, BASE_PATH, "Books-tab-delim.txt", MOST_REVIEWED);
		Preprocessor.createFileWithOnlyKMostActiveUsers(NUMBER_OF_USERS, BASE_PATH, MOST_REVIEWED, MOST_REVIEWED_AND_MOST_ACTIVE);
		Preprocessor.integerizeUserAndProductIds(BASE_PATH, MOST_REVIEWED_AND_MOST_ACTIVE, MOST_REVIEWED_AND_MOST_ACTIVE_WITH_INTEGER_IDS);
		
		ArrayList<IntegerRating> allRatings = new ArrayList<IntegerRating>();
		allRatings.addAll(Preprocessor.readIntegerRatingsFromFile(BASE_PATH, MOST_REVIEWED_AND_MOST_ACTIVE_WITH_INTEGER_IDS));
		
		
				
		//UVDecomp uvd = new UVDecomp();
		//double rmsle = uvd.crossValidateAndReturnRMSLE(allRatings, 10);
		//System.out.println("UVDecomp rmsle: " + rmsle);
		
		NaiveBayes naiveBayes = new NaiveBayes();
		//double naiveBayesRMSLE = naiveBayes.crossValidateAndReturnRMSLE(allRatings, 10);
		
		double naiveBayesAccuracy = naiveBayes.crossValidateAndReturnAccuracy(allRatings, 10);
		System.out.println("NaiveBayes accuracy: " + naiveBayesAccuracy);
		
		System.out.println("Ran in " + watch.getElapsedSeconds() + " seconds.");
		
		// required for UVDecomp
		//ArrayList<Rating> allRatings = readFileIntoRatingsList();
		
		// Map userIds to their ratings.
		//HashMap<String, Rating> trainingRatingsByUserId = new HashMap<String, Rating>();
		// Map productIds to their ratings.
		//HashMap<String, Rating> trainingRatingsByProductId = new HashMap<String, Rating>();


	}
	

	
	public static ArrayList<Rating> readFileIntoRatingsList() {
		
		return new ArrayList<Rating>();
		
	}
}
