package play.modules.elasticsearch.mapping.impl;

import play.db.Model;
import play.modules.elasticsearch.mapping.MapperFactory;

/**
 * ModelMapper for play.db.Model subclasses.
 * 
 * @param <M>
 *            the model type
 */
public class PlayModelMapper<M extends Model> extends AnyClassMapper<M> {

	public PlayModelMapper(MapperFactory factory, Class<M> clazz, String indexPrefix) {
		super(factory, clazz, indexPrefix);
	}

	@Override
	public String getDocumentId(M model) {
		return String.valueOf(model._key());
	}

}
