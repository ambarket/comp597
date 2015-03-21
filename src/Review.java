class Review implements Comparable<Review> {
	String userID;
	String rating;
	String helpfulness;

	public Review(String userID, String rating, String helpfulness) {
		this.userID = userID;
		this.rating = rating;
		this.helpfulness = helpfulness;
	}

	@Override
	public int compareTo(Review o) {
		return userID.compareTo(o.userID);
	}
}