package aub.edu.lb.spark.optimization.rules;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SparkMap;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;
import aub.edu.lb.spark.optimization.model.SparkMapValues;

public class ExplorationRule extends Rule {

	private Flow transformation;
	
	public ExplorationRule(int id, Flow flow, Configuration configuration) {
		super(id, configuration);
		transformation = flow;
	}
	
	public Job getAltJob(Job job) {
		Job newJob = new Job(job.getAction().getClone());
		newJob.getAction().setInput(getAltFlow(job.getAction().getInput()));
		return newJob;
	}
	
	private Flow getAltFlow(Flow flow) {
		if(flow instanceof DataSource) { return flow.getClone(); }
		if(flow == transformation) {
			Flow newFlow = substitute();
			newFlow.setInput(getAltFlow(newFlow.getInput()));
			return newFlow;
		}
		Flow newFlow = flow.getClone();
		if(flow instanceof SingleRDDTransformation) {
			newFlow.setInput(getAltFlow(flow.getInput()));
		}
		else if(flow instanceof DoubleRDDTransformation) {
			newFlow.setInput1(getAltFlow(flow.getInput1()));
			newFlow.setInput2(getAltFlow(flow.getInput2()));
		}
		return newFlow;
	}
	
	private Flow substitute() {
		if(getId() == MapMapValuesSubstitution || getId() == MapValuesMapSubstitution || getId() == FlatMapMapValuesSubstitution || getId() == MapValuesGroupByKeySubstitution) {
			return new SparkMap(transformation.getInput(), getConfiguration().getFunctionsManipulation().changeFunctionDomain(((SparkMapValues) transformation).getUDF()));
		}
		return null;
	}
	
}
