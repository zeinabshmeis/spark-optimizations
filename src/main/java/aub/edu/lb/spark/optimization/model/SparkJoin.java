package aub.edu.lb.spark.optimization.model;
public class SparkJoin extends DoubleRDDTransformation{

	public SparkJoin(Flow flow1, Flow flow2) {
		super(flow1, flow2);
	}
	
	private SparkJoin(boolean visited) {
		super(null, null);
		setVisited(visited);
	}

	public Flow getClone() { return new SparkJoin(super.isVisited()); }
	
	public String toString() { return "join( ) º ( (" + getInput1().toString() + " ) • (" + getInput2().toString() + " ) )"; }
}
