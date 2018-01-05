package aub.edu.lb.spark.optimization.strategies;

import java.util.ArrayList;
import java.util.Map;

import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.optimizer.Optimizer.Edge;

public interface SelectionStrategy {

	public Job selectJob(Job originalJob, Map<Job, ArrayList<Edge>> alternatives);
}
