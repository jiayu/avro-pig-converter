package me.jamc.converter.avroToPig;

public class FieldBean {

	private String path;
	private String type;
	private int level;
	
	
	public FieldBean(String path, String type, int level){
		this.path = path;
		this.type = type;
		this.level = level;
	}
	
	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	
}
