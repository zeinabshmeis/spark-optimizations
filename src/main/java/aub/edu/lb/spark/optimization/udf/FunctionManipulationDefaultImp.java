package aub.edu.lb.spark.optimization.udf;

import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Pair;

public class FunctionManipulationDefaultImp implements FunctionManipulation{

	@Override
	public <V> Predicate<V> composePredicates(Predicate<V> predicate1,
			Predicate<V> predicate2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V, T, U> UnaryOperator<V, U> composeUDFs(UnaryOperator<V, T> udf1,
			UnaryOperator<T, U> udf2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V, U, K> UnaryOperator<V, U> changeFunctionDomain(
			UnaryOperator<V, U> udf) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V, U, K> Flow transformUDFToFlow(
			UnaryOperator<Pair<K, V>, Pair<K, U>> udf) {
		// TODO Auto-generated method stub
		return null;
	}

}
