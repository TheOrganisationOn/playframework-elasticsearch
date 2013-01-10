package play.modules.elasticsearch;

import io.searchbox.client.JestResult;
import io.searchbox.core.Search;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import play.db.Model;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;
import play.modules.elasticsearch.transformer.JPATransformer;

import com.google.common.collect.Lists;

/**
 * An elastic search query
 * 
 * @param <T>
 *            the generic model to search for
 */
public class Query<T extends Model> {

	private final Class<T> clazz;
	private final QueryBuilder builder;
	private final List<AbstractFacetBuilder> facets;
	private final List<SortBuilder> sorts;

	private int from = -1;
	private int size = -1;

	private boolean hydrate = false;
	private boolean useMapper = false;

	Query(Class<T> clazz, QueryBuilder builder) {
		Validate.notNull(clazz, "clazz cannot be null");
		Validate.notNull(builder, "builder cannot be null");
		this.clazz = clazz;
		this.builder = builder;
		this.facets = new ArrayList<AbstractFacetBuilder>();
		this.sorts = new ArrayList<SortBuilder>();
	}

	/**
	 * Sets from
	 * 
	 * @param from
	 *            record index to start from
	 * @return self
	 */
	public Query<T> from(int from) {
		this.from = from;

		return this;
	}

	/**
	 * Sets fetch size
	 * 
	 * @param size
	 *            the fetch size
	 * @return self
	 */
	public Query<T> size(int size) {
		this.size = size;

		return this;
	}

	/**
	 * Controls entity hydration
	 * 
	 * @param hydrate
	 *            hydrate entities
	 * @return self
	 */
	public Query<T> hydrate(boolean hydrate) {
		this.hydrate = hydrate;

		return this;
	}

	/**
	 * Controls the usage of mapper
	 * 
	 * @param useMapper
	 *            use mapper during result processing
	 * @return self
	 */
	public Query<T> useMapper(boolean useMapper) {
		this.useMapper = useMapper;

		return this;
	}

	/**
	 * Adds a facet
	 * 
	 * @param facet
	 *            the facet
	 * @return self
	 */
	public Query<T> addFacet(AbstractFacetBuilder facet) {
		Validate.notNull(facet, "facet cannot be null");
		facets.add(facet);

		return this;
	}

	/**
	 * Sorts the result by a specific field
	 * 
	 * @param field
	 *            the sort field
	 * @param order
	 *            the sort order
	 * @return self
	 */
	public Query<T> addSort(String field, SortOrder order) {
		Validate.notEmpty(field, "field cannot be null");
		Validate.notNull(order, "order cannot be null");
		sorts.add(SortBuilders.fieldSort(field).order(order));

		return this;
	}

	/**
	 * Adds a generic {@link SortBuilder}
	 * 
	 * @param sort
	 *            the sort builder
	 * @return self
	 */
	public Query<T> addSort(SortBuilder sort) {
		Validate.notNull(sort, "sort cannot be null");
		sorts.add(sort);

		return this;
	}

	/**
	 * Runs the query
	 * 
	 * @return the search results
	 */
	public SearchResults<T> fetch() {
		ModelMapper<T> mapper = ElasticSearchPlugin.getMapper(clazz);
		String index = mapper.getIndexName();
		Search search = new Search(Search.createQueryWithBuilder(builder.toString()));
		search.addIndex(index);

		try {
			JestResult result = ElasticSearchPlugin.getJestClient().execute(search);
			List<WithId> withIds = result.getSourceAsObjectList(WithId.class);
			List<Object> ids = Lists.newArrayList();
			for (WithId withId : withIds) {
				System.err.println("got object with id " + withId.id);
				ids.add(withId.id);
			}
			if (ids.isEmpty() == false) {
				List<T> modelObjects = JPATransformer.loadFromDb(clazz, ids);
				// TODO sort by order
				return new SearchResults<T>(modelObjects.size(), modelObjects, null);
			} else {
				return new SearchResults<T>(0L, Lists.<T> newArrayList(), null);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		//
		// // Build request
		// SearchRequestBuilder request = ElasticSearch.builder(builder, clazz);
		//
		// // Facets
		// for (AbstractFacetBuilder facet : facets) {
		// request.addFacet(facet);
		// }
		//
		// // Sorting
		// for (SortBuilder sort : sorts) {
		// request.addSort(sort);
		// }
		//
		// // Paging
		// if (from > -1) {
		// request.setFrom(from);
		// }
		// if (size > -1) {
		// request.setSize(size);
		// }
		//
		// // Only load id field for hydrate
		// if (hydrate) {
		// request.addField("_id");
		// }
		//
		// if (Logger.isDebugEnabled()) {
		// Logger.debug("ES Query: %s", builder.toString());
		// }
		//
		// SearchResponse searchResponse = request.execute().actionGet();
		// SearchResults<T> searchResults = null;
		// if (hydrate) {
		// searchResults = new JPATransformer<T>().toSearchResults(searchResponse, clazz);
		// } else if (useMapper) {
		// searchResults = new MapperTransformer<T>().toSearchResults(searchResponse, clazz);
		// } else {
		// searchResults = new SimpleTransformer<T>().toSearchResults(searchResponse, clazz);
		// }
		// return searchResults;
	}
}
