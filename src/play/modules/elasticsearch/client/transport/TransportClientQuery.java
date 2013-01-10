package play.modules.elasticsearch.client.transport;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import play.Logger;
import play.db.Model;
import play.modules.elasticsearch.ElasticSearch;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.search.SearchResults;
import play.modules.elasticsearch.transformer.JPATransformer;
import play.modules.elasticsearch.transformer.MapperTransformer;
import play.modules.elasticsearch.transformer.SimpleTransformer;

public class TransportClientQuery<T extends Model> extends Query<T> {

	public TransportClientQuery(Class<T> clazz, QueryBuilder builder) {
		super(clazz, builder);
	}

	@Override
	public SearchResults<T> fetch() {

		// Build request
		SearchRequestBuilder request = ElasticSearch.builder(builder, clazz);

		// Facets
		for (AbstractFacetBuilder facet : facets) {
			request.addFacet(facet);
		}

		// Sorting
		for (SortBuilder sort : sorts) {
			request.addSort(sort);
		}

		// Paging
		if (from > -1) {
			request.setFrom(from);
		}
		if (size > -1) {
			request.setSize(size);
		}

		// Only load id field for hydrate
		if (hydrate) {
			request.addField("_id");
		}

		if (Logger.isDebugEnabled()) {
			Logger.debug("ES Query: %s", builder.toString());
		}

		SearchResponse searchResponse = request.execute().actionGet();
		SearchResults<T> searchResults = null;
		if (hydrate) {
			searchResults = new JPATransformer<T>().toSearchResults(searchResponse, clazz);
		} else if (useMapper) {
			searchResults = new MapperTransformer<T>().toSearchResults(searchResponse, clazz);
		} else {
			searchResults = new SimpleTransformer<T>().toSearchResults(searchResponse, clazz);
		}
		return searchResults;
	}

}
