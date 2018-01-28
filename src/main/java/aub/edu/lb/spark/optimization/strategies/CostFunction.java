package aub.edu.lb.spark.optimization.strategies;

import aub.edu.lb.spark.optimization.model.Job;

public interface CostFunction {

	public double getCost(Job job);
}
