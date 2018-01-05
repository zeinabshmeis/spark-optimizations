package aub.edu.lb.spark.optimization.optimizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import aub.edu.lb.spark.optimization.model.Job;


public class SearchSpace {

	private HashMap<Job, ArrayList<Edge>> graph;
	
	public SearchSpace() { graph = new HashMap<>(); }
	
	public void addJob(Job job) { graph.putIfAbsent(job, new ArrayList<>()); }
	
	public void addEdge(Job from, Job to, int rule) {
		graph.putIfAbsent(from, new ArrayList<>());
		graph.putIfAbsent(to, new ArrayList<>());
		graph.get(from).add(new Edge(from, to, rule));
	}
	
	public ArrayList<Edge> getNeighbors(Job job) { return graph.get(job); }
	
	public Set<Job> getJobs() { return graph.keySet(); }
	
	public HashMap<Job, ArrayList<Edge>> getSearchSpace() { return graph; }
	
	public static class Edge {
		Job from, to;
		int rule;
		
		Edge(Job from, Job to, int rule) {
			this.from = from;
			this.to = to;
			this.rule = rule;
		}
		
		public String toString() { return "Edge from: " + from.toString() + " and to: " + to.toString() + " by applyign the rule: " + rule; }
	}
}
