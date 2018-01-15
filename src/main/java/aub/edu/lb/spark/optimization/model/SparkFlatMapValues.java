package aub.edu.lb.spark.optimization.model;
import java.util.Iterator;

import aub.edu.lb.spark.optimization.udf.*;


public class SparkFlatMapValues extends SingleRDDTransformation {
	
	private UDF udf;

	public SparkFlatMapValues(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkFlatMapValues(UDF function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkFlatMapValues(udf, super.isVisited()); }
	
	public String toString() { return "flatMapValues( " + udf + " ) º " + getInput().toString(); }
}
