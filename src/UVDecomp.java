import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class UVDecomp {
	
	int MAX_FEATURES = 40;
	int MAX_EPOCHS = 120;
	double LEARNING_RATE = 0.001;
	double CORRECTION = 0.02;	// What should this be?
	
	// Create these onse since size doesn't change
	double[][] userFeature = new double[MAX_FEATURES][FinalAssignment.NUMBER_OF_USERS];
	double[][] productFeature = new double[MAX_FEATURES][FinalAssignment.NUMBER_OF_PRODUCTS];
	
	// Maybe a waste of space, all that really used is ratingBaseLines but shouldn't be too bad.
	double[] productCorrectedAverages = new double[FinalAssignment.NUMBER_OF_PRODUCTS];
	double[] averageRatingOffsets = new double[FinalAssignment.NUMBER_OF_USERS];
	double[][] ratingBaseLines =  new double[FinalAssignment.NUMBER_OF_PRODUCTS][FinalAssignment.NUMBER_OF_USERS];
	
	double globalAverage;
	
	// FinalAssignment.allRatings - leaveOutForPrediction
	HashSet<IntegerRating> trainingRatings;
	
	public double crossValidateAndReturnRMSE(ArrayList<IntegerRating> allRatings, int numberOfFolds) {
		int ratingsPerFold = allRatings.size() / numberOfFolds;
		
		ArrayList<HashSet<IntegerRating>> folds = new ArrayList<HashSet<IntegerRating>>();
		int i;
		HashSet<IntegerRating> tmp = new HashSet<IntegerRating>();
		for (i = 0; i < numberOfFolds-1; i++) {
			tmp = new HashSet<IntegerRating>();
			tmp.addAll(allRatings.subList(i * ratingsPerFold, (i+1) * ratingsPerFold));
			folds.add(tmp);
		}
		tmp = new HashSet<IntegerRating>();
		tmp.addAll(allRatings.subList(i * ratingsPerFold, allRatings.size()));
		folds.add(tmp);
		
		double sumOfRMSE = 0.0;
		
		i=0;
		StopWatch watch = new StopWatch().start();
		for (HashSet<IntegerRating> leaveOutForPrediction : folds) {
			buildAndTrainNewModel(allRatings, leaveOutForPrediction);
			
			double rmse = 0;
			for (IntegerRating target : leaveOutForPrediction) {
				double error =  target.ratingValue - predict(target.userId, target.productId);
				rmse += error * error;
				System.out.println(error);
			}
			rmse /= leaveOutForPrediction.size();
			rmse = Math.sqrt(rmse);
			sumOfRMSE += rmse;
			
			
			System.out.println("Cross-val of fold " + i + " finished after " + watch.getElapsedSeconds() + " seconds with RMSE " + rmse);
			i++;
		}
		
		return sumOfRMSE / folds.size();
	}
	
	private void buildAndTrainNewModel(ArrayList<IntegerRating> allRatings, HashSet<IntegerRating> leaveOutForPrediction) {
		
		trainingRatings = new HashSet<IntegerRating>();
		globalAverage = 0.0;
		HashMap<Integer, HashSet<IntegerRating>> trainingUserRatings = new HashMap<Integer, HashSet<IntegerRating>>();
		HashMap<Integer, SumCountAverage> productSumCountAverages = new HashMap<Integer, SumCountAverage>();

		for (IntegerRating rating : allRatings) {
			if (!leaveOutForPrediction.contains(rating)) {
				trainingRatings.add(rating);
				
				if (!trainingUserRatings.containsKey(rating.userId)) {
					trainingUserRatings.put(rating.userId, new HashSet<IntegerRating>());
				}
				trainingUserRatings.get(rating.userId).add(rating);

				if (!productSumCountAverages.containsKey(rating.productId)) {
					productSumCountAverages.put(rating.productId, new SumCountAverage(0,0));
				}
				productSumCountAverages.get(rating.productId).addValue(rating.ratingValue);
				
				globalAverage += rating.ratingValue;
			}
		}
		
		System.out.println(allRatings.size() + " " + leaveOutForPrediction.size() + " " + trainingRatings.size());
		
		globalAverage /= trainingRatings.size();
		
		precomputeRatingBaselines(productSumCountAverages, trainingUserRatings);

		// Reset the feature vectors to 0.
		for (int feature = 0; feature < MAX_FEATURES; feature++) {
			for (int user = 0; user < FinalAssignment.NUMBER_OF_USERS; user++) {
				userFeature[feature][user] = 0;
			}
			
			for (int product = 0; product < FinalAssignment.NUMBER_OF_PRODUCTS; product++) {
				productFeature[feature][product] = 0;
			}
		}
		
		//System.out.println("Users: " + trainingUserRatings.keySet().size());
		//System.out.println("Products: " + productSumCountAverages.keySet().size());
		
		trainModel();
	}
	
	private void precomputeRatingBaselines(HashMap<Integer, SumCountAverage> productSumCountAverages, HashMap<Integer, HashSet<IntegerRating>> trainingUserRatings) {
		// Precompute corrected product averages for all users
		for (int i = 0; i < FinalAssignment.NUMBER_OF_PRODUCTS; i++) {
			if (productSumCountAverages.containsKey(i)) {
				productCorrectedAverages[i] = productSumCountAverages.get(i).getCorrectedAvg(globalAverage, CORRECTION); 
			} else {
				productCorrectedAverages[i] = 0;
			}
		}
		
		// Precompute averageRatingOffsets for all users
		for (int userId = 0; userId < FinalAssignment.NUMBER_OF_USERS; userId++) {
			averageRatingOffsets[userId] = 0.0;
			if (trainingUserRatings.containsKey(userId) && trainingUserRatings.get(userId) .size() > 0) {
				for (IntegerRating rating : trainingUserRatings.get(userId)) {
					averageRatingOffsets[userId] += productCorrectedAverages[rating.productId] - rating.ratingValue;
				}
				
				 averageRatingOffsets[userId] /= trainingUserRatings.get(userId).size();
			}
		}
		
		// Precompute rating baselines for all product-user pairs
		for (int productId = 0; productId < FinalAssignment.NUMBER_OF_PRODUCTS; productId++) {
			for (int userId = 0; userId < FinalAssignment.NUMBER_OF_USERS; userId++) {
				ratingBaseLines[productId][userId] = productCorrectedAverages[productId] + averageRatingOffsets[userId];
			}
		}
	}
	
	private void trainModel() {		
		StopWatch watch = new StopWatch().start();
		for (int feature = 0; feature < MAX_FEATURES; feature++) {
			for (int user = 0; user < FinalAssignment.NUMBER_OF_USERS; user++) {
				userFeature[feature][user] = 0.1;
			}
			for (int product = 0; product < FinalAssignment.NUMBER_OF_PRODUCTS; product++) {
				productFeature[feature][product] = 0.1;
			}
			
			for (int epoch = 0; epoch < MAX_EPOCHS; epoch++) {
				for (IntegerRating rating : trainingRatings) {
					double err = rating.ratingValue - predict(rating.userId, rating.productId);
					double oldUserValue = userFeature[feature][rating.userId];
					double oldProdValue = productFeature[feature][rating.productId];
					
					userFeature[feature][rating.userId] += (LEARNING_RATE * (err * oldProdValue - CORRECTION * oldUserValue)) ;
					productFeature[feature][rating.productId] += (LEARNING_RATE * (err * oldUserValue - CORRECTION * oldProdValue)) ;
				}
			}
		}
	}
	
	public double predict(int userId, int productId) {
		// User or product isn't in the training set we have no information to base a prediction on.
		if (averageRatingOffsets[userId] == 0 || productCorrectedAverages[productId] == 0) {
			return globalAverage;
		}
		
		double retval = ratingBaseLines[productId][userId];
		
		for (int feature = 0; feature < MAX_FEATURES; feature++)  {
			retval += userFeature[feature][userId] * productFeature[feature][productId];
		}
		
		//if (retval > 5) { retval = 5; }
		//if (retval < 1) { retval = 1; }
		return retval;
	}
	
	/* OLD METHODS
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
		
		i=0;
		for (HashSet<IntegerRating> fold : folds) {
			StopWatch watch = new StopWatch().start();
			
			buildAndTrainNewModel(fold);
			
			System.out.println("Build of model for fold " + i + " finished in " + watch.getElapsedSeconds());
			
			double accuracy = 0;
			for (IntegerRating target : fold) {
				double prediction = predict(target.userId, target.productId);
				if (prediction < 1) prediction = 1;
				if (prediction > 5) prediction = 5;
				if (target.ratingValue == prediction) {
					accuracy++;
				}
			}
			accuracy /= fold.size();

			sumOfAccuracy += accuracy;
			if (sumOfAccuracy < 0) {
				System.out.println("Overflow? " + sumOfAccuracy);
			}
			
			System.out.println("Cross-val of fold " + i + " finished in " + watch.getElapsedSeconds() + " with accuracy " + accuracy);
			i++;
		}
		
		return sumOfAccuracy / folds.size();
	}
	
	private void buildAndTrainNewModel(HashSet<IntegerRating> leaveOutForPrediction, int maxFeatures, int maxEpochs, double learningRate, double correction) {
		MAX_FEATURES = maxFeatures;
		MAX_EPOCHS = maxEpochs;
		LEARNING_RATE = learningRate;
		CORRECTION = correction;	
		
		buildAndTrainNewModel(leaveOutForPrediction);
	}
	*/
}
