package aub.edu.lb.spark.optimization.rules;
import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SparkMap;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;

public class GroupByAggregateRule extends Rule{

	private Flow transformation;
	
	public GroupByAggregateRule(int id, Flow flow, Configuration configuration) {
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
			getLast(newFlow).setInput(getAltFlow(transformation.getInput().getInput()));
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
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Flow substitute() {
		if(getId() == GroupByAggregate) {
			return getConfiguration().getFunctionsManipulation().transformUDFToFlow((((SparkMap) transformation).getUDF()));
		}
		return null;
	}
	
	private Flow getLast(Flow flow) {
		while(flow.getInput() != null) flow = flow.getInput();
		return flow;
	}
	
}
