package me.jamc.converter.avroToPig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.ObjectContext;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This class is used for converting an object into a pig Tuple. The
 * tree structure of the tuple should remain the same as the object.
 * The basic type converting rule is below If the current attribute is primitive
 * type (other than CharSequence), just return it If the current attribute is a
 * CharSequence (Avro stores String as CharSequence which Pig does not
 * understand), return String If the current attribute is a List, return a
 * DataBag If the current attribute is a Map, return a Map (at the moment, our
 * map stores only String, so we don't need to due with this) If the current
 * attribute is a Object we define, return a Tuple
 * 
 * @author jamesji
 * 
 */
public class AvroTupleConverter {

	private static final JexlEngine jexl = new JexlEngine();
	private AvroBeanTree tree;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private BagFactory mBagFactory = BagFactory.getInstance();
	private Map<String, Byte> fieldTypeMap = new HashMap<String, Byte>();

	public AvroTupleConverter(String className) throws ClassNotFoundException{
		tree = new AvroBeanTree(className);
	}
	
	/**
	 * for building up a schema
	 * 
	 * @return
	 */
	public Schema generateSchema() {
		fillTypeMap();
		return generateSchemaByLevel(tree.getListOfFields(), 0);
	}

	private void fillTypeMap() {
		fieldTypeMap.put("java.util.List", DataType.BAG);
		fieldTypeMap.put("java.lang.Boolean", DataType.BOOLEAN);
		fieldTypeMap.put("java.lang.String", DataType.CHARARRAY);
		fieldTypeMap.put("java.lang.CharSequence", DataType.CHARARRAY);
		fieldTypeMap.put("java.lang.Byte", DataType.BYTE);
		fieldTypeMap.put("java.lang.Double", DataType.DOUBLE);
		fieldTypeMap.put("java.lang.Float", DataType.FLOAT);
		fieldTypeMap.put("java.lang.Integer", DataType.INTEGER);
		fieldTypeMap.put("java.lang.Long", DataType.LONG);
		fieldTypeMap.put("java.util.Map", DataType.MAP);

		// Don't know what type to map in Java yet
		// fieldTypeMap.put("", DataType.BIGCHARARRAY);
		// fieldTypeMap.put("", DataType.INTERNALMAP);
		// fieldTypeMap.put("", DataType.BIGINTEGER);
		// fieldTypeMap.put("", DataType.BIGDECIMAL);
		// fieldTypeMap.put("", DataType.ERROR);
		// fieldTypeMap.put("", DataType.DATETIME);
		// fieldTypeMap.put("", DataType.BYTEARRAY);

	}

	private Schema generateSchemaByLevel(Queue<FieldBean> queue, int level) {
		Schema s = new Schema();
		int index = 0;

		while (!queue.isEmpty()) {
			if (level > queue.peek().getLevel())
				return s;

			FieldBean bean = queue.poll();
			Schema.FieldSchema field;
			try {
				if (!fieldTypeMap.containsKey(bean.getType())) {
					field = new Schema.FieldSchema(
							getCurrentPath(bean.getPath()),
							generateSchemaByLevel(queue, level + 1),
							DataType.TUPLE);
				} else if (bean.getType().equals("java.util.List")) {
					field = new Schema.FieldSchema(
							getCurrentPath(bean.getPath()),
							generateSchemaByLevel(queue, level + 1),
							DataType.BAG);

				} else {
					field = new Schema.FieldSchema(
							getCurrentPath(bean.getPath()),
							fieldTypeMap.get(bean.getType()));
				}
			} catch (Exception e) {
				field = null;
			}

			s.add(field);
		}

		return s;
	}

	/**
	 * for converting object into a tuple
	 * 
	 * @param object
	 * @return
	 */
	public Tuple convert(Object obj) {
		JexlContext context = new ObjectContext(jexl,obj);

		return mTupleFactory.newTuple(makeAList(tree.getListOfFields(),
				context, 0));
	}

	private List<Object> makeAList(Queue<FieldBean> queue,
			JexlContext context, int level) {
		List<Object> list = new LinkedList<Object>();

		while (!queue.isEmpty()) {
			if (level > queue.peek().getLevel())
				return list;

			FieldBean bean = queue.poll();
			convertFieldToTuple(queue, context, level, list, bean);

		}

		return list;
	}

	private void convertFieldToTuple(Queue<FieldBean> queue,
			JexlContext context, int level, List<Object> list,
			FieldBean bean) {
		if (isNotNull(bean, context)) {
			if (bean.getType().equals("java.lang.CharSequence")) {
				list.add(jexl.createExpression(bean.getPath()).evaluate(context).toString());
			} else if (bean.getType().startsWith("java.lang")) {
				list.add(jexl.createExpression(bean.getPath()).evaluate(context));
			} else if (bean.getType().equals("java.util.List")) {

				List<Object> valueList = (List<Object>)jexl.createExpression(bean.getPath()).evaluate(context);
				String path = getCurrentPath(bean.getPath());
				DataBag bag = mBagFactory.newDefaultBag();

				Queue<FieldBean> subQueue = new LinkedList<FieldBean>();

				List<FieldBean> subList = generateSubList(queue, path);

				for (int i = 0; i < valueList.size(); i++) {
					for (int j = 0; j < subList.size(); j++) {
						FieldBean b = new FieldBean(subList.get(j).getPath()
								.replace(path, path + "[" + i + "]"), subList
								.get(j).getType(), subList.get(j).getLevel());
						subQueue.add(b);
					}
					bag.add(mTupleFactory.newTuple(makeAList(subQueue, context,
							level + 1)));
				}
				list.add(bag);

			} else if (bean.getType().equals("java.util.Map")) {
				list.add((Map)jexl.createExpression(bean.getPath()).evaluate(context));
			} else {
				list.add(mTupleFactory.newTuple(makeAList(queue, context,
						level + 1)));
			}
		} else {
			list.add(getEmptyObject(bean));
			removedSubPathFromQueue(getCurrentPath(bean.getPath()), queue);
		}

	}

	/**
	 * return empty object for corresponding type
	 * 
	 * @param bean
	 * @return
	 */
	private Object getEmptyObject(FieldBean bean) {
		if (bean.getType().equals("java.util.Map")) {
			return new HashMap();
		} else if (bean.getType().equals("java.util.List")) {
			return mBagFactory.newDefaultBag(new ArrayList());
		} else {
			return null;
		}

	}

	/**
	 * generate a subList for with all subpath from the current path
	 * 
	 * @param queue
	 * @param path
	 * @return
	 */
	private List<FieldBean> generateSubList(Queue<FieldBean> queue, String path) {
		List<FieldBean> subList = new LinkedList<FieldBean>();
		while (!queue.isEmpty()) {
			if (queue.peek().getPath().contains(path)) {
				subList.add(queue.poll());
			} else {
				break;
			}
		}
		return subList;
	}

	/**
	 * Check if the field in the bean is null
	 * 
	 * @param bean
	 * @param context
	 * @return
	 */
	private boolean isNotNull(FieldBean bean, JexlContext context) {
		return !(Boolean)jexl.createExpression(bean.getPath() + " == null ").evaluate(context);
	}

	/**
	 * removing sub-paths from current path
	 */
	private void removedSubPathFromQueue(String path, Queue<FieldBean> queue) {
		while (!queue.isEmpty()) {
			if (queue.peek().getPath().contains(path)) {
				queue.poll();
			} else {
				break;
			}
		}
	}

	/**
	 * return current path 
	 * 
	 * @param path
	 * @return
	 */
	private String getCurrentPath(String path) {
		return path.substring(path.lastIndexOf(".") + 1);
	}
}
