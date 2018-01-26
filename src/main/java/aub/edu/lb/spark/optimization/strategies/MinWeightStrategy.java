package aub.edu.lb.spark.optimization.strategies;

import java.util.ArrayList;
import java.util.Map;
import java.util.PriorityQueue;

import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.optimizer.Optimizer.Edge;

public class MinWeightStrategy implements SelectionStrategy{

	private WeightFunction weightFunction;
	private boolean pruning = false;
	
	public MinWeightStrategy(WeightFunction weightFunction, boolean pruning) {
		this.weightFunction = weightFunction;
		this.pruning = pruning;
	}
	
	@Override
	public Job selectJob(Job originalJob, Map<Job, ArrayList<Edge>> alternatives) {
		return Dijkstra(originalJob, alternatives);
	}
	
	public Job Dijkstra(Job job, Map<Job, ArrayList<Edge>> alternatives) {
		PriorityQueue<Node> queue = new PriorityQueue<>();
		queue.add(new Node(job, 0));
		Job minJob = job;
		int minWeight = Integer.MAX_VALUE;
		while(!queue.isEmpty()) {
			Node front = queue.poll();
			if(alternatives.get(front.job).size() == 0 && front.weight < minWeight) {
				minJob = front.job;
				minWeight = front.weight;
			}
			if(pruning) {
				int bestWeight = Integer.MAX_VALUE;
				for(Edge edge: alternatives.get(front.job)) bestWeight = Math.min(bestWeight, weightFunction.getWeight(edge.rule));
				for(Edge edge: alternatives.get(front.job)) {
					if(weightFunction.getWeight(edge.rule) == bestWeight) {
						queue.add(new Node(edge.to, front.weight + weightFunction.getWeight(edge.rule)));
					}
				}
			}
			else {
				for(Edge edge: alternatives.get(front.job)) {
					queue.add(new Node(edge.to, front.weight + weightFunction.getWeight(edge.rule)));
				}
			}
		}
		return minJob;
	}

	private static class Node implements Comparable<Node>{
		Job job;
		int weight;
		public Node(Job job, int weight) {
			this.job = job;
			this.weight = weight;
		}
		@Override
		public int compareTo(Node o) {
			if(weight < o.weight) return -1;
			if(weight > o.weight) return 1;
			return 0;
		}
	}
}
