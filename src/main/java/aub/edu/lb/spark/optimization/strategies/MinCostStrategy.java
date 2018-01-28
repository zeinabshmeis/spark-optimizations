package aub.edu.lb.spark.optimization.strategies;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.optimizer.Optimizer.Edge;

public class MinCostStrategy implements SelectionStrategy{

	private CostFunction costFunction;
	private boolean pruning = false;
	
	public MinCostStrategy(CostFunction costFunction, boolean pruning) {
		this.costFunction = costFunction;
		this.pruning = pruning;
	}

	@Override
	public Job selectJob(Job originalJob, Map<Job, ArrayList<Edge>> alternatives) {
		return BFS(originalJob, alternatives);
	}

	public Job BFS(Job job, Map<Job, ArrayList<Edge>> alternatives) {
		PriorityQueue<Node> queue = new PriorityQueue<>();
		queue.add(new Node(job, costFunction.getCost(job)));
		
		HashSet<Job> visited = new HashSet<>();
		visited.add(job);
		
		Job minJob = job;
		double minCost = Double.MAX_VALUE;
		
		while(!queue.isEmpty()) {
			Node front = queue.poll();
			if( front.cost <= minCost) {
				minJob = front.job;
				minCost = front.cost;
			}
			if(pruning) {
				double bestCost = Double.MAX_VALUE;
				for(Edge edge: alternatives.get(front.job)) {
					bestCost = Math.min(bestCost, costFunction.getCost(edge.to));
				}
				for(Edge edge: alternatives.get(front.job)) {
					double cost = costFunction.getCost(edge.to);
					if(cost == bestCost) {
						queue.add(new Node(edge.to, cost));
					}
				}
			}
			else {
				for(Edge edge: alternatives.get(front.job)) {
					if(!visited.contains(edge.to)) {
						queue.add(new Node(edge.to, costFunction.getCost(edge.to)));
						visited.add(edge.to);
					}
				}
			}
		}
		return minJob;
	}

	private static class Node implements Comparable<Node>{
		Job job;
		double cost;
		public Node(Job job, double cost) {
			this.job = job;
			this.cost = cost;
		}
		@Override
		public int compareTo(Node o) {
			if(cost < o.cost) return -1;
			if(cost > o.cost) return 1;
			return 0;
		}
	}
	
}
