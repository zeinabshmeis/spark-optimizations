package aub.edu.lb.spark.optimization.strategies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.optimizer.Optimizer.Edge;

public class MaxWeightStrategy implements SelectionStrategy{

	private WeightFunction weightFunction;
	private boolean pruning = false;
	
	public MaxWeightStrategy(WeightFunction weightFunction, boolean pruning) {
		this.weightFunction = weightFunction;
		this.pruning = pruning;
	}
	
	@Override
	public Job selectJob(Job originalJob, Map<Job, ArrayList<Edge>> alternatives) {
		return Dijkstra(originalJob, alternatives);
	}
	
	private Job Dijkstra(Job job, Map<Job, ArrayList<Edge>> graph) {
		PriorityQueue<Node> queue = new PriorityQueue<>();
		queue.add(new Node(job, 0));
		Job maxJob = job;
		int maxWeight = 0;
		HashMap<Job, Integer> visited = new HashMap<>();
		visited.put(job, 0);
		while(!queue.isEmpty()) {
			Node front = queue.poll();
			if(graph.get(front.job).size() == 0 && front.weight > maxWeight) {
				maxJob = front.job;
				maxWeight = front.weight;
			}
			if(pruning) {
				int bestWeight = Integer.MIN_VALUE;
				for(Edge edge: graph.get(front.job)) bestWeight = Math.max(bestWeight, weightFunction.getWeight(edge.rule));
				for(Edge edge: graph.get(front.job)) {
					if(weightFunction.getWeight(edge.rule) == bestWeight) {
						queue.add(new Node(edge.to, front.weight + weightFunction.getWeight(edge.rule)));
					}
				}
			}
			else {
				for(Edge edge: graph.get(front.job)) {
					int newWeight = front.weight + weightFunction.getWeight(edge.rule);
					if(!visited.containsKey(edge.to) || newWeight > visited.get(edge.to)) {
						queue.add(new Node(edge.to, newWeight));
						visited.put(edge.to, newWeight);
					}
				}
			}
		}
		return maxJob;
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
			if(weight < o.weight) return 1;
			if(weight > o.weight) return -1;
			return 0;
		}
	}
}
