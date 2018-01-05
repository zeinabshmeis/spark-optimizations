package aub.edu.lb.spark.optimization.model;
public class TextFile implements DataSource {
	
	private String fileName;
	private boolean visited = false;
	
	public TextFile(String file) {
		fileName = file;
	}
	
	public TextFile(String file, boolean visited) {
		fileName = file;
		this.visited = visited;
	}
	
	public String getFileName() { return fileName; }
	public void setFileName(String file) {fileName = file; }

	public Flow getClone() { return new TextFile(fileName, visited); }

	public boolean isVisited() { return visited; }

	public void setVisited(boolean visited) { this.visited = visited; }

	public Flow getInput() { return null; }
	public Flow getInput1() { return null; }
	public Flow getInput2() { return null; }
	public void setInput(Flow flow) { }
	public void setInput1(Flow flow) { }
	public void setInput2(Flow flow) { }
	
	public String toString() { return "textFile( " + fileName + " )"; }
	
}
