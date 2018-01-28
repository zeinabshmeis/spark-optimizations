package aub.edu.lb.spark.optimization.strategies;

import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;

public class CostFunctionGreedyImp implements CostFunction{

	@Override
	public double getCost(Job job) {
		return 1 + getCost(job.getAction().getInput());
	}

	private double getCost(Flow flow) {
		if(flow instanceof DataSource) return 1;
		if(flow instanceof SingleRDDTransformation) return getUnitCost(flow) + getCost(flow.getInput());
		return getUnitCost(flow) + getCost(flow.getInput1()) + getCost(flow.getInput2());
	}
	
	private double getUnitCost(Flow flow) {
		return 1;
	}
}
