package aub.edu.lb.spark.optimization.model;
import java.util.Iterator;

import aub.edu.lb.spark.optimization.udf.Functions;
import aub.edu.lb.spark.optimization.udf.UnaryOperator;


public class SparkFlatMap<V, U> extends SingleRDDTransformation {

	private UnaryOperator<V, Iterator<U>> udf;

	public SparkFlatMap(Flow flow, UnaryOperator<V, Iterator<U>> function) {
		super(flow);
		udf = function;
	}
	
	private SparkFlatMap(UnaryOperator<V, Iterator<U>> function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UnaryOperator<V, Iterator<U>> getUDF() { return udf; }
	public void setUDF(UnaryOperator<V, Iterator<U>> function) { udf = function; }
	
	public Flow getClone() { return new SparkFlatMap<V, U>(udf, super.isVisited()); }
	
	public String toString() { return "flatMap( " + Functions.unaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
}
