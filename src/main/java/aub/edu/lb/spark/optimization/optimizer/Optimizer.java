package aub.edu.lb.spark.optimization.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;
import aub.edu.lb.spark.optimization.rules.Rule;

public class Optimizer {
	
	private Job originalJob;
	private Map<Job, ArrayList<Edge>> altJobGraph;
	private Configuration configuration;
	
	public Optimizer(Job job, Configuration configuration) {
		originalJob = job;
		altJobGraph = new HashMap<>();
		altJobGraph.put(originalJob, new ArrayList<>());
		this.configuration = configuration;
	}
	
	public Job getOriginalJob() { return originalJob; }
	public Set<Job> getAlternativeJobs() { return altJobGraph.keySet(); }
	
	public Configuration getConfiguration() { return configuration; } 
	public void setConfiguration(Configuration configuration) { this.configuration = configuration; }

	private void addEdge(Job from, Job to, int rule) {
		altJobGraph.putIfAbsent(from, new ArrayList<>());
		altJobGraph.putIfAbsent(to, new ArrayList<>());
		altJobGraph.get(from).add(new Edge(from, to, rule));
	}
	
	
	public void synthesis() { optimize(originalJob); }
	
	private void optimize(Job job) {
		optimizeFlow(job.getAction().getInput(), job);
		for(Edge edge: altJobGraph.get(job)) {
			optimize(edge.to);
		}
	}
	
	private void optimizeFlow(Flow flow, Job job) {
		if(flow instanceof DataSource) return;
		if(!flow.isVisited()) {
			ArrayList<Rule> rules = RuleMatcher.getMatches(flow, configuration);
			for(Rule rule: rules) {
				Job newJob = rule.getAltJob(job);
				addEdge(job, newJob, rule.getId());
			}
		}
		flow.setVisited(true);
		if(flow instanceof SingleRDDTransformation) {
			optimizeFlow(flow.getInput(), job);
		}
		else if(flow instanceof DoubleRDDTransformation) {
			optimizeFlow(flow.getInput1(), job);
			optimizeFlow(flow.getInput2(), job);
		}
	}
	
	public static class Edge {
		public Job from, to;
		public int rule;
		
		Edge(Job from, Job to, int rule) {
			this.from = from;
			this.to = to;
			this.rule = rule;
		}
		
		public String toString() { return "Edge from: " + from.toString() + " and to: " + to.toString() + " by applyign the rule: " + rule; }
	}
	
}
