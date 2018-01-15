package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.*;

public class SparkReduceByKey extends SingleRDDTransformation {
	
	private UDF udf;
	
	public SparkReduceByKey(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkReduceByKey(UDF function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkReduceByKey(udf, super.isVisited()); }

	public String toString() { return "reduceByKey( " + udf + " ) º " + getInput().toString(); }
}
