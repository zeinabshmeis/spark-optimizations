package aub.edu.lb.spark.optimization.rules;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.Job;

public abstract class Rule {
	
	public static final int EliminateEmptyTransformation = 1;
	public static final int MapMapFusion = 2;
	public static final int FlatMapMapFusion = 3;
	public static final int MapValuesMapValuesFusion = 4;
	public static final int FlatMapValuesMapValuesFusion = 5;
	public static final int FilterFilterFusion = 6;
	public static final int ReduceByKeyMapReorder = 7;
	public static final int FilterSTRedorder = 8;
	public static final int FilterJoinReorder1 = 9;
	public static final int FilterJoinReorder2 = 10;
	public static final int FilterJoinReorder3 = 11;
	public static final int FilterSetOpReorder = 12; 
	public static final int GroupByAggregate = 13;
	public static final int ReduceTransformationReorder = 14; 
	public static final int MapMapValuesSubstitution = 15;
	public static final int MapValuesMapSubstitution = 16;
	public static final int FlatMapMapValuesSubstitution = 17;
	public static final int MapValuesGroupByKeySubstitution = 18;
	
	private int ruleId; 
	private Configuration configuration;
	
	public Rule(int id, Configuration configuration) { 
		this.ruleId = id; 
		this.configuration = configuration;
	}
	
	public int getId() { return ruleId; }
	public Configuration getConfiguration() { return configuration; }
	
	public abstract Job getAltJob(Job job);

}
