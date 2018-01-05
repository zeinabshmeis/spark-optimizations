package aub.edu.lb.spark.optimization.model;
import java.util.Iterator;

import aub.edu.lb.spark.optimization.udf.Functions;
import aub.edu.lb.spark.optimization.udf.UnaryOperator;


public class SparkFlatMapValues<K, V, U> extends SingleRDDTransformation {
	
	private UnaryOperator<V, Iterator<U>> udf;

	public SparkFlatMapValues(Flow flow, UnaryOperator<V, Iterator<U>> function) {
		super(flow);
		udf = function;
	}
	
	private SparkFlatMapValues(UnaryOperator<V, Iterator<U>> function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UnaryOperator<V, Iterator<U>> getUDF() { return udf; }
	public void setUDF(UnaryOperator<V, Iterator<U>> function) { udf = function; }

	public Flow getClone() { return new SparkFlatMapValues<K, V, U>(udf, super.isVisited()); }
	
	public String toString() { return "flatMapValues( " + Functions.unaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
}
