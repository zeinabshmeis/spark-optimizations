package aub.edu.lb.spark.optimization.strategies;

import aub.edu.lb.spark.optimization.rules.Rule;

public class WeightFunctionImp implements WeightFunction{

	@Override
	public int getWeight(int ruleId) {
		if(ruleId == Rule.FilterFilterFusion) return 1;
		else if(ruleId == Rule.MapMapFusion) return 2;
		else if(ruleId == Rule.MapMapValuesFusion) return 2;
		else if(ruleId == Rule.MapValuesMapFusion) return 2;
		else if(ruleId == Rule.FlatMapMapFusion) return 2;
		else if(ruleId == Rule.FlatMapMapValuesFusion) return 2;
		else if(ruleId == Rule.MapValuesMapValuesFusion) return 2;
		else if(ruleId == Rule.FlatMapValuesMapValuesFusion) return 2;
		else if(ruleId == Rule.FilterSTRedorder) return 5;
		else if(ruleId == Rule.ReduceByKeyMapReorder) return 6;
		else if(ruleId == Rule.ReduceTransformationReorder) return 6;
		else if(ruleId == Rule.EliminateEmptyTransformation) return 6;
		else if(ruleId == Rule.FilterJoinReorder1) return 8;
		else if(ruleId == Rule.FilterJoinReorder2) return 8;
		else if(ruleId == Rule.FilterJoinReorder3) return 8;
		else if(ruleId == Rule.FilterSetOpReorder) return 8;
		else if(ruleId == Rule.GroupByAggregate_Map) return 10;
		else if(ruleId == Rule.GroupByAggregate_MapValues) return 10;
		return 0;
	}

}
