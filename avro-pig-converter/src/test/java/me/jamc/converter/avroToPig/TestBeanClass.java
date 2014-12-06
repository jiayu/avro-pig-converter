package me.jamc.converter.avroToPig;

public class TestBeanClass {

	public String attributeString;
	public Integer attributeInt;
	public Long attributeLong;
	public Double attributeDouble;
	
	
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
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TestBeanClass){
			TestBeanClass that = (TestBeanClass)obj;
			if(that.attributeDouble == this.attributeDouble 
					&& that.attributeInt == this.attributeInt
					&& that.attributeLong == this.attributeLong
					&& that.attributeString.equals(this.attributeString))
				return true;
		}
		
		return false;
	}
	public static TestBeanClass getTestBeanInstance(){
		return new TestBeanClass().setAttributeInt(Integer.MAX_VALUE)
				.setAttributeLong(Long.MAX_VALUE)
				.setAttributeDouble(Double.MAX_VALUE)
				.setAttributeString("this is a string");
	}
}
