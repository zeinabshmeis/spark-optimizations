package aub.edu.lb.spark.optimization.udf;

public interface UnaryOperator<V, U> {

	U apply(V x);
	
}
