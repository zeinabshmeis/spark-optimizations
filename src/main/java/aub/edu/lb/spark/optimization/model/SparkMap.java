package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.*;

public class SparkMap extends SingleRDDTransformation {
	
	private UDF udf;

	public SparkMap(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkMap(UDF function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkMap(udf, super.isVisited()); }
	
	public String toString() { return "map( " + udf + " ) º " + getInput().toString(); }
	
}
