package aub.edu.lb.spark.optimization.model;

public class SparkGroupByKey extends SingleRDDTransformation {
	
	public SparkGroupByKey(Flow flow) {
		super(flow);
	}
	
	private SparkGroupByKey() {
		super(null);
	}

	public Flow getClone() { return new SparkGroupByKey(); }
	
	public String toString() { return "GroupByKey( ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkGroupByKey) return true;
		return false;
	}
	
	@Override
	public int hashCode() {
		return 127;
	}
}
