package clustering;
import java.util.HashSet;

class ProductCount implements Comparable<ProductCount> {
	String productID;
	String productTitle;
	String cleanTitle;
	int reviewCount;
	HashSet<String> usersReviewedBy;

	public ProductCount(String productID, String productTitle, String cleanTitle, int reviewCount, String firstUserID) {
		this.productID = productID;
		this.productTitle = productTitle;
		this.cleanTitle = cleanTitle;
		this.reviewCount = reviewCount;
		this.usersReviewedBy = new HashSet<String>();
		usersReviewedBy.add(firstUserID);
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
		ProductCount other = (ProductCount) that;
		return productID.equals(other.productID);
	}
	
	public boolean similar(ProductCount other) {
		return cleanTitle.equals(other.cleanTitle) || (cleanTitle.contains(other.cleanTitle) || other.cleanTitle.contains(cleanTitle) && this.reviewCount == other.reviewCount);
	}
}
