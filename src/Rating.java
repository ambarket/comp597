class Rating {
	String userId, productId, title;
	int ratingValue;
	
	public Rating(String userId, String productId, int ratingValue) {
		this.userId = userId;
		this.productId = productId;
		this.ratingValue = ratingValue;
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
			Rating test = (Rating)obj;
			return userId.equals(test.userId) && productId.equals(test.productId);
	}
	
	
	public int hashCode()
	{
		int hash = 7;
		hash = 31 * hash + userId.hashCode();
		hash = 31 * hash + productId.hashCode();
		return hash;
	}
}

class IntegerRating {
	int userId, productId, ratingValue;
	
	public IntegerRating(int userId, int productId, int ratingValue) {
		this.userId = userId;
		this.productId = productId;
		this.ratingValue = ratingValue;
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
			IntegerRating test = (IntegerRating)obj;
			return userId == test.userId && productId == test.productId;
	}
	
	
	public int hashCode()
	{
		return Integer.hashCode(userId) + (7 * Integer.hashCode(productId));
	}
}