package aub.edu.lb.spark.optimization.rules;
import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SparkMap;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;
import aub.edu.lb.spark.optimization.model.SparkMapValues;
import aub.edu.lb.spark.optimization.udf.FunctionManipulation;

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
	
	private Flow substitute() {
		FunctionManipulation funcMan = getConfiguration().getFunctionsManipulation();
		if(getId() == GroupByAggregate_Map) {
			return funcMan.transformUDFToFlow((((SparkMap) transformation).getUDF()));
		}
		else if(getId() == GroupByAggregate_MapValues) {
			return funcMan.transformUDFToFlow(funcMan.changeFunctionDomain((((SparkMapValues) transformation).getUDF())));
		}
		return null;
	}
	
	private Flow getLast(Flow flow) {
		while(flow.getInput() != null) flow = flow.getInput();
		return flow;
	}
	
}
