package org.giscience.osmMeasures.repository;

import org.apache.commons.lang3.tuple.Pair;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.measures.tools.Lineage;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

public class MeasureResidentialRoads extends MeasureOSHDB<Number, OSMEntitySnapshot> {

/*
    @Override
    public Boolean refersToTimeSpan() {
        return false;
    }

    @Override
    public Integer defaultDaysBefore() {
        return 3 * 12 * 30;
    }

    @Override
    public Integer defaultIntervalInDays() {
        return 30;
    }
*/

    @Override
    public SortedMap<GridCell, Number> compute(MapAggregator<GridCell, OSMEntitySnapshot> mapReducer, OSHDBRequestParameter p) throws Exception {

        // Connect to database and create tagTranslator
        OSHDBJdbc oshdb = (OSHDBJdbc) this.getOSHDB();
        TagTranslator tagTranslator = new TagTranslator(oshdb.getConnection());
        OSHDBTag tag = tagTranslator.getOSHDBTagOf("highway", "residential");

        // EXAMPLE ONLY - PLEASE INSERT CODE HERE
        return Cast.result(Index.map(mapReducer
            .osmTag("highway")
            //.aggregateBy(sn -> sn.getEntity().hasTagValue(tag.getKey(), tag.getValue()))
            .map(x -> {
              double residential = x.getEntity().hasTagValue(tag.getKey(), tag.getValue()) ? 1. : 0.;
              double len = Geo.lengthOf(x.getGeometryUnclipped());
              return Pair.of(residential * len, len);
            })
            .reduce(new IdentitySupplier(), new Accumulator(), new Combiner()),
            x -> x.getLeft() / x.getRight()));
    }

  private static class IdentitySupplier implements SerializableSupplier<Pair<Double, Double>> {
    @Override
    public Pair<Double, Double> get() {
      return Pair.of(0., 0.);
    }
  }

  private static class Accumulator implements
      SerializableBiFunction<Pair<Double, Double>, Pair<Double, Double>, Pair<Double, Double>> {
    @Override
    public Pair<Double, Double> apply(Pair<Double, Double> t, Pair<Double, Double> u) {
      return Pair.of(t.getKey() + u.getKey(), t.getRight() + u.getRight());
    }
  }

  private static class Combiner implements SerializableBinaryOperator<Pair<Double, Double>> {
    @Override
    public Pair<Double, Double> apply(Pair<Double, Double> t, Pair<Double, Double> u) {
      return Pair.of(t.getKey() + u.getKey(), t.getRight() + u.getRight());
    }
  }

}
