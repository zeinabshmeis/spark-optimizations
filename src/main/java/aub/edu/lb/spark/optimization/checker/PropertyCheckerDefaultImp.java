package aub.edu.lb.spark.optimization.checker;

import aub.edu.lb.spark.optimization.model.*;
import aub.edu.lb.spark.optimization.udf.*;

/**
 * 
 * This should be the default implementation for checking for properties
 *
 */
public class PropertyCheckerDefaultImp implements PropertyChecker{

	@Override
	public boolean isIdentityOperation(UDF operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDistributive(UDF operation1, UDF operation2) {
		if(operation2.toString().equals("sum") && operation1.toString().equals("(devideNminusOne_WithKey)")) return true;
		return false;
	}

	@Override
	public boolean hasReadWriteConflict(Flow flow1, Flow flow2) {
		if(flow1 instanceof SparkFilter && ((SparkFilter)flow1).getUDF().toString().equals("filterMidNight")) {
			if(flow2 instanceof SparkMap && (((SparkMap)flow2).getUDF().toString().equals("extractHourAgencyLocation") 
					|| ((SparkMap)flow2).getUDF().toString().equals("split--extractHourAgencyLocation") || ((SparkMap)flow2).getUDF().toString().equals("extractHour") 
					|| ((SparkMap)flow2).getUDF().toString().equals("split--extractHour"))) 
				return true;
			else return false;
		}
		return true;
	}

	@Override
	public boolean readSetIsSubsetOfVarSet(Flow flow1, Flow flow2) {
		if(flow1 instanceof SparkFilter && (flow2 instanceof SparkMapValues || flow2 instanceof SparkMapValues) && ((SparkFilter)flow1).getUDF().toString().equals("filterDistance")) return true;
		if(flow1 instanceof SparkFilter && flow2 instanceof SparkReduceByKey && ((SparkFilter)flow1).getUDF().toString().equals("filterHourCount")) return true;
		if(flow1 instanceof SparkFilter && ((SparkFilter)flow1).getUDF().toString().equals("filterMidNight")) return true;
		return false;
	}

}
