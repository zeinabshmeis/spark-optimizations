package aub.edu.lb.spark.optimization.checker;

import aub.edu.lb.spark.optimization.udf.FunctionManipulation;

/**
 * 
 * This class represents the configuration composed of the 
 * property checker and the function manipulator
 * 
 */
public class Configuration {	
	
	/**
	 * This field represents the object that contains all the methods 
	 * used for checking if a certain property is satisfied
	 */
	private PropertyChecker propertyChecker;
	
	/**
	 * This field represents the object that contains all the methods 
	 * used for composing and converting functions 
	 */
	private FunctionManipulation functionManipulation;
	
	/**
	 * 
	 * @param propertyChecker instance of the PropertyChecker class
	 * @param functionManipulation instance of the FunctionManipulation class
	 */
	public Configuration(PropertyChecker propertyChecker, FunctionManipulation functionManipulation) {
		this.propertyChecker = propertyChecker;
		this.functionManipulation = functionManipulation;
	}
	
	/**
	 * 
	 * @return the PropertyChecker object
	 */
	public PropertyChecker getPropertyChecker() { return propertyChecker; }
	
	/**
	 * 
	 * @param propertyChecker instance of the PropertyChecker class
	 */
	public void setPropertyChecker(PropertyChecker propertyChecker) { this.propertyChecker = propertyChecker; }
	
	/**
	 * 
	 * @return the functionManipulation object
	 */
	public FunctionManipulation getFunctionsManipulation() { return functionManipulation; }
	
	/**
	 * 
	 * @param functionManipulation instance of the FunctionManipulation class
	 */
	public void setFunctionManipulation(FunctionManipulation functionManipulation) { this.functionManipulation = functionManipulation; }
	
}
