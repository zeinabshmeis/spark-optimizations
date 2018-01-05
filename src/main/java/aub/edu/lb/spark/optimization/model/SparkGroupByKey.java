package aub.edu.lb.spark.optimization.model;
public class SparkGroupByKey<K, V> extends SingleRDDTransformation {
	
	protected SparkGroupByKey(Flow flow) {
		super(flow);
	}
	
	private SparkGroupByKey(boolean visited) {
		super(null);
		setVisited(visited);
	}

	public Flow getClone() { return new SparkGroupByKey<K, V>(super.isVisited()); }
	
	public String toString() { return "GroupByKey( ) º " + getInput().toString(); }
}
