package me.jamc.converter.avroToPig;

import java.util.HashMap;

import junit.framework.Assert;
import me.jamc.converter.avroToPig.TestBeanClass.SubBeanClass;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroTupleConverter {
    private Logger logger = LoggerFactory.getLogger(getClass());
    
	AvroTupleConverter converter;
	TestBeanClass bean;
	TupleFactory tFactory = TupleFactory.getInstance();
	BagFactory bFactory = BagFactory.getInstance();
	
	@Before
	public void setUp() throws Exception {
		converter = new AvroTupleConverter("me.jamc.converter.avroToPig.TestBeanClass");
		bean = TestBeanClass.getTestBeanInstance();
	}

	@Test
	public void testWithFullTuple() throws ClassNotFoundException, ExecException {
		assertTest();
	}
	
	@Test
	public void testWithNullDoubleFieldTuple() throws ClassNotFoundException, ExecException {	
		bean.setAttributeDouble(null);
		assertTest();
	}
	
	@Test
	public void testWithNullStringFieldTuple() throws ClassNotFoundException, ExecException {	
		bean.setAttributeString(null);
		assertTest();
	}
	
	@Test
	public void testWithNullIntFieldTuple() throws ClassNotFoundException, ExecException {	
		bean.setAttributeInt(null);
		assertTest();
	}
	
	@Test
	public void testWithNullLongFieldTuple() throws ClassNotFoundException, ExecException {	
		bean.setAttributeLong(null);
		assertTest();
	}
	
	@Test
	public void testWithNullListFieldTuple() throws ClassNotFoundException, ExecException {		
		bean.attributeList = null;
		assertTest();	
	}
	
	@Test
	public void testWithNullMapFieldTuple() throws ClassNotFoundException, ExecException {
		bean.attributeMap = null;
		assertTest();
	}
	
	private void assertTest() throws ClassNotFoundException, ExecException{
		Tuple expected = tFactory.newTuple(6);
		int index = 0;
		expected.set(index++, bean.attributeString);
		expected.set(index++, bean.attributeInt);
		expected.set(index++, bean.attributeLong);
		expected.set(index++, bean.attributeDouble);
		
		
		//add a bag which was a list in the bean
		DataBag bag = bFactory.newDefaultBag();
		if(bean.attributeList !=null){
			for(SubBeanClass s : bean.attributeList){
				Tuple t = tFactory.newTuple(1);
				t.set(0, s.attributeString);
				bag.add(t);
			}
		}	
		expected.set(index++, bag);
		
		//add a map
		if(bean.attributeMap != null){
			expected.set(index, bean.attributeMap);
		}else{
			expected.set(index, new HashMap());
		}
		
		
		logger.info("Expected Tuple:");
		logger.info(expected.toString());
		
		Tuple actual = converter.convert(bean);
		logger.info("Actual Tuple:");
		logger.info(actual.toString());
		
		Assert.assertEquals(expected, actual);
	}
	
	/**
	 * Schema Test
	 */
	@Test
	public void testSchemaGenerated(){
		String expectedSchema = "{attributeString: chararray,attributeInt: int,attributeLong: long,attributeDouble: double,attributeList: {attributeString: chararray},attributeMap: map[]}";
		Assert.assertEquals(expectedSchema.toString(), converter.generateSchema().toString());
	}

}
