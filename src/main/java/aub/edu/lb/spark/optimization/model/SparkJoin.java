package aub.edu.lb.spark.optimization.model;
public class SparkJoin extends DoubleRDDTransformation{

	public SparkJoin(Flow flow1, Flow flow2) {
		super(flow1, flow2);
	}
	
	private SparkJoin() {
		super(null, null);
	}

	public Flow getClone() { return new SparkJoin(); }
	
	public String toString() { return "join( ) • ( (" + getInput1().toString() + " ) ; (" + getInput2().toString() + " ) )"; }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkJoin) return true;
		return false;
	}
	
	@Override
	public int hashCode() {
		return 167;
	}
}
