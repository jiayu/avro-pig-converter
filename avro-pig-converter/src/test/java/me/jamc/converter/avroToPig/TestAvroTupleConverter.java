package me.jamc.converter.avroToPig;

import junit.framework.Assert;
import me.jamc.converter.avroToPig.TestBeanClass.SubBeanClass;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Ignore;
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
	public void testConvertedTuple() throws ClassNotFoundException, ExecException {
		
		Tuple expected = tFactory.newTuple(6);
		int index = 0;
		expected.set(index++, bean.attributeString);
		expected.set(index++, bean.attributeInt);
		expected.set(index++, bean.attributeLong);
		expected.set(index++, bean.attributeDouble);
		//add a bag which was a list in the bean
		DataBag bag = bFactory.newDefaultBag();
		for(SubBeanClass s : bean.attributeList){
			Tuple t = tFactory.newTuple(1);
			t.set(0, s.attributeString);
			bag.add(t);
		}
		expected.set(index++, bag);
		//add a map
		expected.set(index, bean.attributeMap);
		
		logger.info("Expected Tuple:");
		logger.info(expected.toString());
		
		Tuple actual = converter.convert(bean);
		logger.info("Actual Tuple:");
		logger.info(actual.toString());
		
		Assert.assertEquals(expected, actual);
	}
	
	/**
	 * this test does not pass which is odd
	 */
	@Test
	public void testSchemaGenerated(){
		String expectedSchema = "{attributeString: chararray,attributeInt: int,attributeLong: long,attributeDouble: double,attributeList: {attributeString: chararray},attributeMap: map[]}";
		Assert.assertEquals(expectedSchema.toString(), converter.generateSchema().toString());
	}

}
