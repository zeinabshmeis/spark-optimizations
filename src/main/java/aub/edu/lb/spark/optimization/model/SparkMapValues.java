package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.*;

public class SparkMapValues extends SingleRDDTransformation {
	
	private UDF udf;

	public SparkMapValues(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkMapValues(UDF function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkMapValues(udf, super.isVisited()); }

	public String toString() { return "mapValues( " + Functions.unaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
}
