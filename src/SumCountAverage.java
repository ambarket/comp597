public class SumCountAverage {
	static long uniqueObjectIds = 0;
	long uniqueId;
	long sum;
	long count;
	
	public SumCountAverage(long sum, long count) {
		uniqueObjectIds++;
		uniqueId = uniqueObjectIds;
		this.sum = sum;
		this.count = count;
	}

	public void addValue(long value) {
		sum += value;
		count++;
	}
	
	public double getSimpleAvg() {
		return sum / (double)count;
	}
	
	public double getCorrectedAvg(double globalAverage, double correction) {
		return (globalAverage * correction + sum) / (count + correction);
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
			SumCountAverage test = (SumCountAverage)obj;
			return uniqueId == test.uniqueId;
	}
	
	public int hashCode()
	{
		return Long.hashCode(uniqueId);
	}
}
