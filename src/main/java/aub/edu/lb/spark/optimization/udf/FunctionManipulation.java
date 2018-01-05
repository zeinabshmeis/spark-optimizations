package aub.edu.lb.spark.optimization.udf;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Pair;

public interface FunctionManipulation {
	
	public <V> Predicate<V> composePredicates(Predicate<V> predicate1, Predicate<V> predicate2);
	public <V, T, U> UnaryOperator<V, U> composeUDFs(UnaryOperator<V, T> udf1, UnaryOperator<T, U> udf2);
	public <V, U, K> UnaryOperator<V, U> changeFunctionDomain(UnaryOperator<V, U> udf);
	public <V, U, K> Flow transformUDFToFlow(UnaryOperator<Pair<K, V>, Pair<K, U>> udf);
	
}
