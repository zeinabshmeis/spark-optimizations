package aub.edu.lb.spark.optimization.model;

public class TextFile implements DataSource {
	
	private String fileName;
	
	public TextFile(String file) {
		fileName = file;
	}
	
	public String getFileName() { return fileName; }
	public void setFileName(String file) {fileName = file; }

	public Flow getClone() { return new TextFile(fileName); }

	public Flow getInput() { return null; }
	public Flow getInput1() { return null; }
	public Flow getInput2() { return null; }
	public void setInput(Flow flow) { }
	public void setInput1(Flow flow) { }
	public void setInput2(Flow flow) { }
	
	public String toString() { return "textFile( " + fileName + " )"; }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TextFile) {
			TextFile textFile = (TextFile) obj;
			return fileName.equals(textFile.getFileName());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 199 * fileName.hashCode();
	}
	
}
