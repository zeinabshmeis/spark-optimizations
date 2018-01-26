package aub.edu.lb.spark.optimization.strategies;

import java.util.ArrayList;
import java.util.Map;
import java.util.PriorityQueue;

import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.optimizer.Optimizer.Edge;

public class MinCostStrategy implements SelectionStrategy{

	private CostFunction costFunction;
	private boolean pruning = false;
	
	public MinCostStrategy(CostFunction costFunction) {
		this.costFunction = costFunction;
	}

	@Override
	public Job selectJob(Job originalJob, Map<Job, ArrayList<Edge>> alternatives) {
		return null;
	}

	public Job Dijkstra(Job job, Map<Job, ArrayList<Edge>> alternatives) {
		PriorityQueue<Job> queue = new PriorityQueue<>();
		queue.add(job);
		Job minJob = job;
		int minCost = Integer.MAX_VALUE;
		while(!queue.isEmpty()) {
			Job front = queue.poll();
			if( front.getCost() < minCost) {
				minJob = front;
				minCost = front.getCost();
			}
			if(pruning) {
				int bestCost = Integer.MAX_VALUE;
				for(Edge edge: alternatives.get(front)) {
					edge.to.setCost(costFunction.getCost(edge.to));
					bestCost = Math.min(bestCost, edge.to.getCost());
				}
				for(Edge edge: alternatives.get(front)) {
					if(edge.to.getCost() == bestCost) {
						queue.add(edge.to);
					}
				}
			}
			else {
				for(Edge edge: alternatives.get(front)) {
					edge.to.setCost(costFunction.getCost(edge.to));
					queue.add(edge.to);
				}
			}
		}
		return minJob;
	}

	
}
