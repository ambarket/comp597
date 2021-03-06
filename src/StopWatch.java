class StopWatch {
    double startTime, endTime;
	
	public StopWatch start() {
		startTime = System.nanoTime();
		return this;
	}
	
	public double getElapsedSeconds() {
		endTime = System.nanoTime();
		return (endTime - startTime) / 1000000000;
	}
	
	public double getElapsedMilliSeconds() {
		endTime = System.nanoTime();
		return (endTime - startTime) / 1000000;
	}
	
	public double getElapsedMicroSeconds() {
		endTime = System.nanoTime();
		return (endTime - startTime) / 1000;
	}
	
	public double getElapsedNanoSeconds() {
		endTime = System.nanoTime();
		return (endTime - startTime);
	}
}