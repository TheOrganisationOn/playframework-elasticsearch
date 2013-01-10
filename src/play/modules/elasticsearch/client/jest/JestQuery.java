package play.modules.elasticsearch.client.jest;

import org.elasticsearch.index.query.QueryBuilder;

import play.db.Model;
import play.modules.elasticsearch.ElasticSearchPlugin;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.search.SearchResults;

public class JestQuery<T extends Model> extends Query<T> {

	public JestQuery(Class<T> clazz, QueryBuilder builder) {
		super(clazz, builder);
	}

	@Override
	public SearchResults<T> fetch() {
		return ElasticSearchPlugin.getClient().searchAndHydrateAll(builder, clazz);
	}

}
