package clustering;
import java.util.HashMap;

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
		return productID.equals(((ProductRecord) that).productID);
	}
	
	public String toString() {
		return this.productID + ": " + this.productTitle + " - " + this.cleanTitle + " : " + this.reviews.size() + " reviews";
	}
}