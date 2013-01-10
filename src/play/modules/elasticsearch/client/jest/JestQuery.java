package play.modules.elasticsearch.client.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;

import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;

import play.db.Model;
import play.modules.elasticsearch.ElasticSearchPlugin;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;
import play.modules.elasticsearch.transformer.JPATransformer;

import com.google.common.collect.Lists;

public class JestQuery<T extends Model> extends Query<T> {

	private JestClient jestClient;

	public JestQuery(Class<T> clazz, QueryBuilder builder, JestClient jestClient) {
		super(clazz, builder);
		this.jestClient = jestClient;
	}

	@Override
	public SearchResults<T> fetch() {
		ModelMapper<T> mapper = ElasticSearchPlugin.getMapper(clazz);
		String index = mapper.getIndexName();
		String queryString = builder.toString();
		System.err.println("queryString " + queryString);
		String jsonQuery = Search.createQueryWithBuilder(queryString);
		System.err.println("queryString json " + jsonQuery);
		// TODO first count and then use the size of count as size here
		// TODO issue a request to jest to have it incorporated into 'search' object
		String sizeQueryString = "{ \"size\" : 1000, \"query\" : " + queryString + "}";
		System.err.println("queryString with size " + sizeQueryString);
		Search search = new Search(sizeQueryString);
		search.addIndex(index);
		try {
			JestResult result = ElasticSearchJestClient.tryToExecute(search, "searching", this.jestClient);
			List<IdOnly> withIds = result.getSourceAsObjectList(IdOnly.class);
			List<Object> ids = Lists.newArrayList();
			for (IdOnly withId : withIds) {
				System.err.println("got object with id " + withId.id);
				ids.add(withId.id);
			}
			if (ids.isEmpty() == false) {
				List<T> modelObjects = JPATransformer.loadFromDb(clazz, ids);
				// TODO sort by order
				return new SearchResults<T>(modelObjects.size(), modelObjects, null);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new SearchResults<T>(0L, Lists.<T> newArrayList(), null);
	}

}
