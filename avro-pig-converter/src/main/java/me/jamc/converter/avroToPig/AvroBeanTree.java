package me.jamc.converter.avroToPig;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * This class is used for building up the class tree using reflection. It flattens  
 * 
 * @author jamesji
 *
 */
public class AvroBeanTree {

	//Converter class should take the queue class only, everytime, it will refresh 
	private Queue<FieldBean> queue = new LinkedList<FieldBean>();
	//The list that holds the leaves list
	private List<FieldBean> list = new LinkedList<FieldBean>();
	
	/**
	 * On initializing of this class, we traverse the tree and pick up all the attributes
	 * @throws ClassNotFoundException 
	 */
	public AvroBeanTree(String className) throws ClassNotFoundException{
		Class<?> m = Class.forName(className);
		
		pushFields(m, "", 0);
	}
	
	/**
	 * This is a recursive function to step through each leave on the tree and records it in the list
	 * @param classType
	 * @param prefix
	 * @param level
	 */
	private void pushFields(Class<?> classType, String prefix, int level){
		
		for(Field f : classType.getFields()){
			//This is from Avro, we will skip it
			if(f.getName().contains("SCHEMA$")) {
				continue;
			}
			
			FieldBean b = new FieldBean(prefix + f.getName(), f.getType().getName(), level);
			list.add(b);
			
			String type = f.getType().getName();
			if(type.startsWith("java.lang")){
				continue;
			}else if(type.equals("java.util.List")){
				pushFields((Class<?>)((ParameterizedType)f.getGenericType()).getActualTypeArguments()[0],prefix + f.getName()+".", level + 1);
//			}else if(type.equals("java.util.Map")){
				//TODO do we need to take care Map?? 
				//currently, key/value in map are all String, so we don't need to care
				//for future, if we have objects stored in map, we need to update this
			}else{
				pushFields(f.getType(), prefix + f.getName()+".", level + 1);
			}
			
		}
	}
	
	/**
	 * This is the only public method used for return a list of fields stored in the queue.
	 * Everytime this method gets called, we need to clear the queue and populate fields from the list.
	 * @return Sample below
	 */
	public Queue<FieldBean> getListOfFields(){
		refreshQueue();
		return queue;
	}
	
	/**
	 * Clear queue and re-populate fields.
	 */
	private void refreshQueue(){
		queue.clear();
		queue.addAll(list);
	}
}
