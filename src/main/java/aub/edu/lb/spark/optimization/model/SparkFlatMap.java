package aub.edu.lb.spark.optimization.model;
import java.util.Iterator;

import aub.edu.lb.spark.optimization.udf.*;


public class SparkFlatMap extends SingleRDDTransformation {

	private UDF udf;

	public SparkFlatMap(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkFlatMap(UDF function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }
	
	public Flow getClone() { return new SparkFlatMap(udf, super.isVisited()); }
	
	public String toString() { return "flatMap( " + udf + " ) º " + getInput().toString(); }
}
