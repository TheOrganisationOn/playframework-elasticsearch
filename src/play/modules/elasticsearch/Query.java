package play.modules.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import play.db.Model;
import play.modules.elasticsearch.search.SearchResults;

/**
 * An elastic search query
 * 
 * @param <T>
 *            the generic model to search for
 */
public abstract class Query<T extends Model> {

	protected final Class<T> clazz;
	protected final QueryBuilder builder;
	protected final List<AbstractFacetBuilder> facets;
	protected final List<SortBuilder> sorts;

	protected int from = -1;
	protected int size = -1;

	protected boolean hydrate = false;
	protected boolean useMapper = false;

	protected Query(Class<T> clazz, QueryBuilder builder) {
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

	public abstract SearchResults<T> fetch();
}
