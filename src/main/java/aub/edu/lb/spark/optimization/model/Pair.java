package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class models a Scala Tuple
 *
 * @param <K> the type of the Key
 * @param <V> the type of the value
 */
public class Pair<K, V>{
	
	public Pair(K key, V value) {
		this.k = key;
		this.v = value;
	}
	
	public K k;
	public V v;

}
