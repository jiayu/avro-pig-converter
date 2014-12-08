package me.jamc.converter.avroToPig;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestBeanClass {

	public String attributeString;
	public Integer attributeInt;
	public Long attributeLong;
	public Double attributeDouble;
	public List<SubBeanClass> attributeList;
	public Map<String,String> attributeMap;
	
	public TestBeanClass setAttributeString(String attributeString) {
		this.attributeString = attributeString;
		return this;
	}

	public TestBeanClass setAttributeInt(Integer attributeInt) {
		this.attributeInt = attributeInt;
		return this;
	}

	public TestBeanClass setAttributeLong(Long attributeLong) {
		this.attributeLong = attributeLong;
		return this;
	}

	public TestBeanClass setAttributeDouble(Double attributeDouble) {
		this.attributeDouble = attributeDouble;
		return this;
	}
	
	public TestBeanClass setAttributeList(SubBeanClass... subBeans){
		attributeList = new LinkedList<SubBeanClass>();
		for(SubBeanClass s : subBeans)
			attributeList.add(s);
		return this;
	}
	
	public TestBeanClass setAttributeMap(Map<String, String> attributeMap) {
		this.attributeMap = attributeMap;
		return this;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TestBeanClass){
			TestBeanClass that = (TestBeanClass)obj;
			if(that.attributeDouble == this.attributeDouble 
					&& that.attributeInt == this.attributeInt
					&& that.attributeLong == this.attributeLong
					&& that.attributeString.equals(this.attributeString)
					&& that.attributeList.equals(this.attributeList)
					&& that.attributeMap.equals(this.attributeMap))
				return true;
		}
		
		return false;
	}
	public static TestBeanClass getTestBeanInstance(){
		return new TestBeanClass().setAttributeInt(Integer.MAX_VALUE)
				.setAttributeLong(Long.MAX_VALUE)
				.setAttributeDouble(Double.MAX_VALUE)
				.setAttributeString("this is a string")
				.setAttributeList(SubBeanClass.getSubBeanInstance("I am subBean No.1"),
						SubBeanClass.getSubBeanInstance("I am subBean No.2"))
				.setAttributeMap(getMapInstance());
	}
	
	private static Map<String,String> getMapInstance(){
		Map<String,String> map = new HashMap<String,String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		return map;
	}
	
	public static class SubBeanClass{
		public String attributeString;

		public SubBeanClass setAttributeString(String attributeString) {
			this.attributeString = attributeString;
			return this;
		}


		@Override
		public boolean equals(Object obj) {
			if(obj instanceof SubBeanClass){
				SubBeanClass that = (SubBeanClass)obj;
				return this.attributeString.equals(that.attributeString);
			}
			return false;
		}
		
		static SubBeanClass getSubBeanInstance(String str){
			return new SubBeanClass().setAttributeString(str);
		}
		
	}
}
