package aub.edu.lb.spark.optimization.model;

public class SparkGroupByKey extends SingleRDDTransformation {
	
	public SparkGroupByKey(Flow flow) {
		super(flow);
	}
	
	private SparkGroupByKey(boolean visited) {
		super(null);
		setVisited(visited);
	}

	public Flow getClone() { return new SparkGroupByKey(super.isVisited()); }
	
	public String toString() { return "GroupByKey( ) º " + getInput().toString(); }
}
