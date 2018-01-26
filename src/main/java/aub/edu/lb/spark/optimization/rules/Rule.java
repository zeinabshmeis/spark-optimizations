package aub.edu.lb.spark.optimization.rules;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.Job;

public abstract class Rule {
	
	public static final int FilterFilterFusion = 1;
	public static final int MapMapFusion = 2;
	public static final int MapMapValuesFusion = 3;
	public static final int MapValuesMapFusion = 4;
	public static final int FlatMapMapFusion = 5;
	public static final int FlatMapMapValuesFusion = 6;
	public static final int MapValuesMapValuesFusion = 7;
	public static final int FlatMapValuesMapValuesFusion = 8;
	public static final int FilterSTRedorder = 9;
	public static final int ReduceByKeyMapReorder = 10;
	public static final int ReduceTransformationReorder = 11;
	public static final int EliminateEmptyTransformation = 12;
	public static final int FilterJoinReorder1 = 13;
	public static final int FilterJoinReorder2 = 14;
	public static final int FilterJoinReorder3 = 15;
	public static final int FilterSetOpReorder = 16; 
	public static final int GroupByAggregate_Map = 17;
	public static final int GroupByAggregate_MapValues = 18;
	
	public static int NUMBER_OF_RULES = 18;
	
	
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
