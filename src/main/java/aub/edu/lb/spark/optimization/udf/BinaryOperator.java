package aub.edu.lb.spark.optimization.udf;

public interface BinaryOperator<V> {
	V apply(V x1, V x2);
}
