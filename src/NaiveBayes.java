import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

class NaiveBayes {
	
	HashMap<Integer, HashSet<IntegerRating>> productToRatingsMap;
	HashMap<Integer, HashSet<IntegerRating>> userToRatingsMap;
	
	int[][][] trainingRatings;
	
	double globalAverage;
	
	public double crossValidateAndReturnAccuracy(ArrayList<IntegerRating> allRatings, int numberOfFolds) {
		Collections.shuffle(allRatings);
		
		int ratingsPerFold = allRatings.size() / numberOfFolds;
		
		ArrayList<HashSet<IntegerRating>> folds = new ArrayList<HashSet<IntegerRating>>();
		int i;
		HashSet<IntegerRating> tmp = new HashSet<IntegerRating>();
		for (i = 0; i < numberOfFolds-1; i++) {
			tmp = new HashSet<IntegerRating>();
			tmp.addAll(allRatings.subList(i * ratingsPerFold, (i+1) * ratingsPerFold));
			folds.add(tmp);
			//System.out.println("created fold " + i + " " + folds.get(i).size());
		}
		tmp = new HashSet<IntegerRating>();
		tmp.addAll(allRatings.subList(i * ratingsPerFold, allRatings.size()));
		folds.add(tmp);
		//System.out.println("created fold " + i + " " + folds.get(i).size());
		
		double sumOfAccuracy = 0.0;
		
		StopWatch watch = new StopWatch();
		i = 0;
		for (HashSet<IntegerRating> leaveOutForPrediction : folds) {			
			watch.start();
			
			buildModel(allRatings, leaveOutForPrediction);
			
			// Make prediction for all the left out ratings. 
			int correct = 0;
			for (IntegerRating target : leaveOutForPrediction) {
				watch.start();
				int prediction = predict(target.userId, target.productId);
				//System.out.println("Actual: " + target.ratingValue + " Prediction: " + prediction );
				if (target.ratingValue == prediction) {
					correct++;
				}
				//System.out.println("Finished prediction " + i + " in " + watch.getElapsedSeconds() + " seconds.");
			}
			//System.out.println("Correct: " + correct + " Predicted: " + leaveOutForPrediction.size() );
			sumOfAccuracy += correct / (double)leaveOutForPrediction.size();
			
			System.out.println("Finished fold " + i + " in " + watch.getElapsedSeconds() + " seconds.");
		}
		
		return sumOfAccuracy / folds.size();
	}
	
	public void buildModel(ArrayList<IntegerRating> allRatings, HashSet<IntegerRating> leaveOutForPrediction) {
		productToRatingsMap = new HashMap<Integer, HashSet<IntegerRating>>();
		userToRatingsMap = new HashMap<Integer, HashSet<IntegerRating>>();
		//trainingRatings = new IntegerRating[FinalAssignment.NUMBER_OF_PRODUCTS][6][];//new HashMap <String, HashMap<String, Integer>>();
		
		// values are userIds that rated the product with value
		trainingRatings = new int[FinalAssignment.NUMBER_OF_PRODUCTS][6][];//new HashMap <String, HashMap<String, Integer>>();
		
		// Map products and users to ratings of/by them, and find global average rating.
		int numberOfRatings = 0;
		for (IntegerRating rating : allRatings) {
			if (!leaveOutForPrediction.contains(rating)) {
				if (!productToRatingsMap.containsKey(rating.productId)) {
					productToRatingsMap.put(rating.productId, new HashSet<IntegerRating>());
				}
				productToRatingsMap.get(rating.productId).add(rating);
				
				if (!userToRatingsMap.containsKey(rating.userId)) {
					userToRatingsMap.put(rating.userId, new HashSet<IntegerRating>());
				}
				userToRatingsMap.get(rating.userId).add(rating);
				
				globalAverage += rating.ratingValue;
				numberOfRatings++;
			}
		}
		globalAverage /= numberOfRatings;
		
		// Pre sort product ratings into buckets by rating value to speed things up later
		for (int productId : productToRatingsMap.keySet()) {
			int[] counts = {productToRatingsMap.get(productId).size(),0,0,0,0,0}; // [0] will store all userIds that rated the product.
			
			for (IntegerRating rating : productToRatingsMap.get(productId)) {
				counts[rating.ratingValue]++;
			}
			// Create the buckets of the right size
			for (int num = 0; num < 6; num++) {
				//trainingRatings[productId][num] = new IntegerRating[counts[num]];
				trainingRatings[productId][num] = new int[counts[num]];
			}
			// Fill up the buckets
			for (IntegerRating rating : productToRatingsMap.get(productId)) {
				assert(counts[rating.ratingValue] > 0);
				counts[rating.ratingValue]--;
				trainingRatings[productId][rating.ratingValue][counts[rating.ratingValue]] = rating.userId;
				
				// Always add to the 0th bucket
				assert(counts[0] > 0);
				counts[0]--;
				trainingRatings[productId][0][counts[0]] = rating.userId;
			}
		}
	}
	
	
	
	public int predict(int userId, int productId) {
		// User isn't in the training set we have no information to base a prediction on.
		if (!productToRatingsMap.containsKey(productId) || !userToRatingsMap.containsKey(userId)) {
			System.out.println("Just returning global average");
			return (int)Math.round(globalAverage);
		}
		
		// Get all product reviews by this user.
		HashSet<IntegerRating> userIdReviews = userToRatingsMap.get(userId);
		
		int ratingValueThatMinimizesSum = 0;
		double minOfLogProbabilitySums = 0.0;	// log[0,1] < 0
		
		for (int ratingValue = 1; ratingValue <= 5; ratingValue++) {
			
			double sumOfLogProbabilities = Math.log(getProbability(productId, ratingValue));
			for (IntegerRating rating : userIdReviews) {
				if (rating.productId != productId) {	
					//System.out.println("Rating Value: " + ratingValue + " " + Math.log(getConditionalProbability(rating.productId, rating.ratingValue, productId, ratingValue)));
					sumOfLogProbabilities += Math.log(getConditionalProbability(rating.productId, rating.ratingValue, productId, ratingValue));
				}
			}

			if (sumOfLogProbabilities < minOfLogProbabilitySums) {
				ratingValueThatMinimizesSum = ratingValue;
				minOfLogProbabilitySums = sumOfLogProbabilities;
			}
		}

		return ratingValueThatMinimizesSum;
	}
	
	// Return P( Rating for productId1 == ratingValue1) based on all available user ratings.
	private double getProbability(int productId1, int ratingValue1) {
		StopWatch watch = new StopWatch().start();
	
		double totalCount = trainingRatings[productId1][0].length + 5;
		
		// Number of users that rated product1 with ratingValue1
		double targetCount = trainingRatings[productId1][ratingValue1].length + 1; // +1 for laplacian correction
				
		//System.out.println("Finished getProb in " + watch.getElapsedSeconds() + " seconds.");
		return targetCount / totalCount;
	}
	
	/* Return P( Rating for productId1 == ratingValue1 | Rating for productId2 == ratingValue2) based on all available user ratings.
	 * 
	 * P( productId1 == ratingValue1 | productId2 == ratingValue2) =
	 * 		count( users who reviewed both productId1 == ratingValue1 and productId2 == ratingValue2) 
	 *		--------------------------------------------------------------------------------------
	 *					count (all users who reviewed productId2 == ratingValue2)
	 */
	private double getConditionalProbability(int productId1, int ratingValue1, int productId2, int ratingValue2) {
		StopWatch watch = new StopWatch().start();
		
		int[] product2Users =  trainingRatings[productId2][ratingValue2];
		
		int[] product1Users = trainingRatings[productId1][ratingValue1];
		
		/* laplacian correction. Is this correct?
		 * +5 for given count because one additional tuple for each class
		 * +1 for conditional count 
		 */
		double givenCount = product2Users.length + 5;
		double conditionalCount = 1;
		
		// TODO: Make structure for this if too slow and if so no need to store userIds anymore in trainingRatings, 
		// just need counts of number of users that rated each product with	each value
		for (int p2UserId : product2Users) {
			for (int p1UserId : product1Users) {
				if (p1UserId == p2UserId) {
					conditionalCount++;
					break;
				}
			}
		}
		
		//System.out.println("Finished getCondProb in " + watch.getElapsedSeconds() + " seconds." + conditionalCount + "/" + givenCount);
		return conditionalCount / givenCount;
	}
}