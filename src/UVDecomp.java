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
	
	
	// Map feature number to a map of userId to value
	//HashMap<Integer, HashMap<String, Double>> userFeature;
	double[][] userFeature;
	// Map feature number to a map of productId to value
	//HashMap<Integer, HashMap<String, Double>> productFeature;
	double[][] productFeature;
	
	// FinalAssignment.allRatings - leaveOutForPrediction
	ArrayList<IntegerRating> allRatings;
	HashSet<IntegerRating> trainingRatings;
	HashMap<Integer, HashSet<IntegerRating>>  trainingUserRatings;
	
	HashMap<Integer, SumCountAverage> productSumCountAverages;
	//HashMap<String, Double>  trainingProductRatings;
	double globalAverage;
	
	public double crossValidateAndReturnRMSLE(ArrayList<IntegerRating> ratings, int numberOfFolds) {
		allRatings = ratings;
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
		
		double sumOfRMSLE = 0.0;
		
		i=0;
		for (HashSet<IntegerRating> fold : folds) {
			StopWatch watch = new StopWatch().start();
			
			buildAndTrainNewModel(fold);
			
			System.out.println("Build of model for fold " + i + " finished in " + watch.getElapsedSeconds());
			
			double rmsle = 0;
			for (IntegerRating target : fold) {
				double error =  Math.log(target.ratingValue) - Math.log(predict(target.userId, target.productId));
				rmsle += error * error;
			}
			rmsle /= fold.size();
			rmsle = Math.sqrt(rmsle);
			
			System.out.println(rmsle);
			sumOfRMSLE += rmsle;
			
			System.out.println("Cross-val of fold " + i + " finished in " + watch.getElapsedSeconds() + " with rmsle " + rmsle);
			i++;
		}
		
		return sumOfRMSLE / folds.size();
	}
	
	public double crossValidateAndReturnAccuracy(ArrayList<IntegerRating> ratings, int numberOfFolds) {
		allRatings = ratings;
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
	
	private void buildAndTrainNewModel(HashSet<IntegerRating> leaveOutForPrediction) {
		System.out.println("Starting to build model");
		
		trainingRatings = new HashSet<IntegerRating>();
		// Make calculation of average rating offset by user easier.
		trainingUserRatings = new HashMap<Integer, HashSet<IntegerRating>> ();
		globalAverage = 0.0;

		// Make it easier and faster to get the corrected averages of a given product's ratings.
		productSumCountAverages = new HashMap<Integer, SumCountAverage>();
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
		
		System.out.println("Users: " + trainingUserRatings.keySet().size());
		System.out.println("Products: " + productSumCountAverages.keySet().size());
		
		globalAverage /= trainingRatings.size();

		
		userFeature = new double[MAX_FEATURES][FinalAssignment.NUMBER_OF_USERS];
		productFeature = new double[MAX_FEATURES][FinalAssignment.NUMBER_OF_PRODUCTS];
		for (int i = 0; i < MAX_FEATURES; i++) {
			for (int user = 0; user < FinalAssignment.NUMBER_OF_USERS; user++) {
				userFeature[i][user] = 0.1;
			}
			
			for (int product = 0; product < FinalAssignment.NUMBER_OF_PRODUCTS; product++) {
				productFeature[i][product] = 0.1;
			}
		}	

		trainModel();

	}
	
	private void trainModel() {		
		System.out.println("Starting to train model");
		StopWatch watch = new StopWatch().start();
		for (int feature = 0; feature < MAX_FEATURES; feature++) {
			double[] userValues = userFeature[feature];
			double[] productValues = productFeature[feature];
			
			for (int epoch = 0; epoch < MAX_EPOCHS; epoch++) {
				for (IntegerRating rating : trainingRatings) {
					double err = rating.ratingValue - predict(rating.userId, rating.productId);
					
					StopWatch watch2 = new StopWatch().start();
					
					double oldUserValue = userValues[rating.userId];
					double oldProdValue = productValues[rating.productId];
					
					userValues[rating.userId] += (LEARNING_RATE * (err * oldProdValue - CORRECTION * oldUserValue)) ;
					productValues[rating.productId] += (LEARNING_RATE * (err * oldUserValue - CORRECTION * oldProdValue)) ;
					
					//System.out.println("Updated value in " + watch2.getElapsedNanoSeconds());

					//System.out.println(oldUserValue + " " + oldProdValue + " " + newUserValue + " " + newProdValue);
				}
			}
			
			System.out.println("Trained " + feature + " features in " + watch.getElapsedSeconds());
		}
		System.out.println("Finished training model");
	}
	
	public double predict(int userId, int productId) {
		// User or product isn't in the training set we have no information to base a prediction on.
		if (!trainingUserRatings.containsKey(userId) || !productSumCountAverages.containsKey(productId)) {
			return globalAverage;
		}
		
		
		double retval = getRatingBaseline(userId, productId);
		
		for (int feature = 0; feature < MAX_FEATURES; feature++)  {
			retval += userFeature[feature][userId] * productFeature[feature][productId];
		}
		
		if (retval > 5) { retval = 5; }
		if (retval < 1) { retval = 1; }
		return retval;
	}

	private double getRatingBaseline(int userId, int productId) {
		return productSumCountAverages.get(productId).getCorrectedAvg(globalAverage, CORRECTION) + getAverageRatingOffsetOfUser(userId);
	}
	
	private double getAverageRatingOffsetOfUser(int userId) {
		double sum = 0.0;
		
		for (IntegerRating rating : trainingUserRatings.get(userId)) {
			sum += productSumCountAverages.get(rating.productId).getCorrectedAvg(globalAverage, CORRECTION) - rating.ratingValue;
		}
		
		return sum / trainingUserRatings.get(userId).size();
	}
}
