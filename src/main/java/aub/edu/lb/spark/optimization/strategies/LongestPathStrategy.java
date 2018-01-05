package aub.edu.lb.spark.optimization.strategies;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.optimizer.Optimizer.Edge;

public class LongestPathStrategy implements SelectionStrategy{

	
	public Job selectJob(Job originalJob, Map<Job, ArrayList<Edge>> alternatives) {
		return BFS(originalJob, alternatives);
	}

	private Job BFS(Job job, Map<Job, ArrayList<Edge>> graph) {
		Job finalJob = job;
		Queue<Job> queue = new LinkedList<Job>();
		queue.add(job);
		while(!queue.isEmpty()) {
			finalJob = queue.poll();
			for(Edge edge: graph.get(finalJob)) { queue.add(edge.to); }
		}
		return finalJob;
	}
}
