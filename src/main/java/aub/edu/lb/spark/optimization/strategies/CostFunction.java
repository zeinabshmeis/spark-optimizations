package aub.edu.lb.spark.optimization.strategies;

import aub.edu.lb.spark.optimization.model.Job;

public interface CostFunction {

	public int getCost(Job job);
}
