package clustering;
import java.util.HashMap;

class ReviewerCount implements Comparable<ReviewerCount> {
	String userID;
	String profileName;
	int reviewCount;
	// ProductId to rating
	HashMap<String, String> productsReviewed;

	public ReviewerCount(String userID, String profileName, int reviewCount, String firstProductID, String firstProductRating) {
		this.userID = userID;
		this.profileName = profileName;
		this.reviewCount = reviewCount;
		productsReviewed = new HashMap<String, String>();
		productsReviewed.put(firstProductID, firstProductRating);
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