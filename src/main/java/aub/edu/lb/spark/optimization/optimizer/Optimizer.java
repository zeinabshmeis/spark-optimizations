package aub.edu.lb.spark.optimization.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.rules.Rule;
import aub.edu.lb.spark.optimization.strategies.SelectionStrategy;

/**
 * 
 * This class represents the optimizer performs the two phases of optimization:
 * Synthesis phase that generates all possible alternative jobs by applying the rewrite rules
 * Selection phase that selects one job from the generated space based on predefined strategy
 *
 */
public class Optimizer {
	
	/**
	 * the original job to optimize
	 */
	private Job originalJob;
	
	/**
	 * the graph representing the generated space with nodes representing the generated jobs 
	 * and edges representing the rules applied
	 */
	private HashMap<Job, ArrayList<Edge>> altJobGraph;
	
	/**
	 * A configuration which is composed of the propoertyChecker and the FunctionManipulation
	 */
	private Configuration configuration;
	
	/**
	 * 
	 * @param job the Spark job that needs to be optimized
	 * @param configuration the implementation used for checking properties and composing functions
	 */
	public Optimizer(Job job, Configuration configuration) {
		originalJob = job;
		altJobGraph = new HashMap<>();
		altJobGraph.put(originalJob, new ArrayList<>());
		this.configuration = configuration;
	}
	
	/**
	 * 
	 * @return the Spark job to be optimized
	 */
	public Job getOriginalJob() { return originalJob; }
	
	/**
	 * 
	 * @return the set of all the generated alternative jobs
	 */
	public Set<Job> getAlternativeJobs() { return altJobGraph.keySet(); }
	
	public Configuration getConfiguration() { return configuration; } 
	public void setConfiguration(Configuration configuration) { this.configuration = configuration; }

	/**
	 * 
	 * @param from the job that the rule is applied on
	 * @param to the job obtained after applying the rule
	 * @param rule the rule that was applied
	 */
	private void addEdge(Job from, Job to, int rule) {
		altJobGraph.putIfAbsent(from, new ArrayList<>());
		altJobGraph.putIfAbsent(to, new ArrayList<>());
		altJobGraph.get(from).add(new Edge(from, to, rule));
	}
	
	/**
	 * this method generates all the alternative jobs 
	 */
	public void synthesis() { optimize(originalJob); }
	
	/**
	 * this method selects the most optimal job based on a specific given strategy
	 * 
	 */
	public Job select(SelectionStrategy strategy) {
		return strategy.selectJob(originalJob, altJobGraph);
	}
	
	/**
	 * this method generates all alternative jobs that can be obtained after applying a single rewrite rule
	 * 
	 * @param job a single job to be optimized
	 */
	private void optimize(Job job) {
		ArrayList<Rule> rules = RuleMatcher.getMatches(job, configuration);
		for(Rule rule: rules) {
			Job newJob = rule.getAltJob(job);
			if(altJobGraph.containsKey(newJob)) {
				altJobGraph.get(job).add(new Edge(job, newJob, rule.getId()));
			}
			else {
				addEdge(job, newJob, rule.getId()); 
				optimize(newJob);
			}
		}
	}
	
	private void optimize2(Job job) {
		for(int ruleId = 1; ruleId <= Rule.NUMBER_OF_RULES; ruleId++) {
			Job newJob = job;
			while(true) {
				Rule rule = RuleMatcher.ruleMatch(newJob, configuration, ruleId);
				if(rule == null) break;
				newJob = rule.getAltJob(newJob);
			}
			if(newJob != job) {
				if(altJobGraph.containsKey(newJob)) {
					altJobGraph.get(job).add(new Edge(job, newJob, ruleId));
				}
				else {
					addEdge(job, newJob, ruleId); 
					optimize(newJob);
				}
			}
		}
	}
	
	/**
	 * 
	 * This class represent the edges of the graph representing the search space of the generated alternative jobs
	 *
	 */
	public static class Edge {
		/**
		 * the job that the rule is applied on
		 */
		public Job from;
		
		/**
		 * the job obtained after applying the rule
		 */
		public Job to;
		
		/**
		 * the rule that was applied
		 */
		public int rule;
		
		Edge(Job from, Job to, int rule) {
			this.from = from;
			this.to = to;
			this.rule = rule;
		}
		
		public String toString() { return "Edge from: " + from.toString() + " and to: " + to.toString() + " by applyign the rule: " + rule; }
	}
	
}
