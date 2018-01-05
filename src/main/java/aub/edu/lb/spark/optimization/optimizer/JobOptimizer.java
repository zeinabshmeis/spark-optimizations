package aub.edu.lb.spark.optimization.optimizer;
import java.util.ArrayList;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;
import aub.edu.lb.spark.optimization.rules.Rule;

public class JobOptimizer {

	private Job job;
	private SearchSpace searchSpace;
	private Configuration configuration;
	
	public JobOptimizer(Job job, SearchSpace searchSpace, Configuration configuration) {
		this.job = job;
		this.searchSpace = searchSpace;
		this.configuration = configuration;
	}
	
	public Job getJob() { return job; }
	public void setjob(Job job) { this.job = job; }
	
	public SearchSpace getSearchSpace() { return searchSpace; }
	public void setSearchSpace(SearchSpace searchSpace) { this.searchSpace = searchSpace; }
	
	public Configuration getConfiguration() { return configuration; } 
	public void setConfiguration(Configuration configuration) { this.configuration = configuration; }
	
	public void optimize() {
		optimizeFlow(job.getAction().getInput());
		for(SearchSpace.Edge edge: searchSpace.getNeighbors(job)) {
			JobOptimizer optimizer = new JobOptimizer(edge.to, searchSpace, configuration);
			optimizer.optimize();
		}
	}
	
	private void optimizeFlow(Flow flow) {
		if (flow instanceof DataSource) return;
		if(!flow.isVisited()) {
			ArrayList<Rule> rules = RuleMatcher.getMatches(flow, configuration);
			for(Rule rule: rules) {
				Job newJob = rule.getAltJob(job);
				searchSpace.addEdge(job, newJob, rule.getId());
			}
		}
		flow.setVisited(true);
		if(flow instanceof SingleRDDTransformation) {
			optimizeFlow(flow.getInput());
		}
		else if(flow instanceof DoubleRDDTransformation) {
			optimizeFlow(flow.getInput1());
			optimizeFlow(flow.getInput2());
		}
	}
	
}
