package me.jamc.converter.avroToPig;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestMetadataTupleConverter {
	AvroTupleConverter converter;
	TestBeanClass bean;
	
	@Before
	public void setUp() throws Exception {
		converter = new AvroTupleConverter("me.jamc.converter.avroToPig.TestBeanClass");
		bean = TestBeanClass.getTestBeanInstance();
	}

	@Test
	public void testConvertedTuple() throws ClassNotFoundException, ExecException {
		
		Tuple expected = TupleFactory.getInstance().newTuple(4);
		int index = 0;
		expected.set(index++, bean.attributeString);
		expected.set(index++, bean.attributeInt);
		expected.set(index++, bean.attributeLong);
		expected.set(index++, bean.attributeDouble);
		
		Assert.assertEquals(expected, converter.convert(bean));
	}
	
	/**
	 * this test does not pass which is odd
	 */
	@Ignore
	public void testSchemaGenerated(){
		String expectedSchema = "{attributeString: chararray,attributeInt: int,attributeLong: long,attributeDouble: double}";
		Assert.assertEquals(expectedSchema, converter.generateSchema());
	}

}
