package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.Functions;
import aub.edu.lb.spark.optimization.udf.UnaryOperator;

public class SparkMapValues<K, V, U> extends SingleRDDTransformation {
	
	private UnaryOperator<V, U> udf;

	public SparkMapValues(Flow flow, UnaryOperator<V, U> function) {
		super(flow);
		udf = function;
	}
	
	private SparkMapValues(UnaryOperator<V, U> function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UnaryOperator<V, U> getUDF() { return udf; }
	public void setUDF(UnaryOperator<V, U> function) { udf = function; }

	public Flow getClone() { return new SparkMapValues<K, V, U>(udf, super.isVisited()); }

	public String toString() { return "mapValues( " + Functions.unaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
}
