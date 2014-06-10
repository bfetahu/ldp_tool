package entities.metadata;

import java.util.HashSet;
import java.util.Set;

public class DBpediaCategories {
	public String category;
	public int level;
	public Set<DBpediaCategories> childs;
	
	public DBpediaCategories(){
		childs = new HashSet<DBpediaCategories>();
	}
	
	public void getRootConceptualCategories(DBpediaCategories dbpc, Set<String> categories){
		if(dbpc.childs.size() > 0){
			for(DBpediaCategories dbpctmp:dbpc.childs){
				getRootConceptualCategories(dbpctmp, categories);
			}
		}
		else{
			categories.add(dbpc.category);
		}
	}
}
