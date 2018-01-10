package aub.edu.lb.spark.optimization.checker;

import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.udf.BinaryOperator;
import aub.edu.lb.spark.optimization.udf.UnaryOperator;

/**
 * 
 * This should be the default implementation for checking for properties
 *
 */
public class PropertyCheckerDefaultImp implements PropertyChecker{

	@Override
	public <V> boolean isIdentityOperation(UnaryOperator<V, V> operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <V> boolean isDistributive(UnaryOperator<V, V> operation1,
			BinaryOperator<V> operation2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasReadWriteConflict(Flow flow1, Flow flow2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean readSetIsSubsetOfVarSet(Flow flow1, Flow flow2) {
		// TODO Auto-generated method stub
		return false;
	}

}
