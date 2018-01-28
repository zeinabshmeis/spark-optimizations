package aub.edu.lb.spark.optimization.strategies;

import aub.edu.lb.spark.optimization.model.*;

public class CostFunctionImp implements CostFunction{

	@Override
	public double getCost(Job job) {
		return getCost(job.getAction().getInput(), 1);
	}
	
	private double getCost(Flow flow, int distance) {
		if(flow instanceof DataSource) return 1;
		if(flow instanceof SingleRDDTransformation) return getUnitCost(flow, distance) + getCost(flow.getInput(), distance + 1);
		return getUnitCost(flow, distance) + getCost(flow.getInput1(), distance + 1) + getCost(flow.getInput2(), distance + 1);
	}
	
	private double getUnitCost(Flow flow,int distance) {
		if(flow instanceof SparkMap) return (((SparkMap)flow).getUDF().getName().split("--").length + 3) * 1 * 1;
		if(flow instanceof SparkMapValues) return (((SparkMapValues)flow).getUDF().getName().split("--").length + 1) * 1 * 1;
		if(flow instanceof SparkFilter) return (((SparkFilter)flow).getUDF().getName().split("--").length) * distance * 1;
		if(flow instanceof SparkReduceByKey) return (((SparkReduceByKey)flow).getUDF().getName().split("--").length + 1) * distance * 2;
		if(flow instanceof SparkJoin) return 1 * (1 / distance) * 3;
		if(flow instanceof SparkGroupByKey) return 1 * 1 * 15;
		return 1;
	}

}
