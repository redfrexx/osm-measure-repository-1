package org.giscience.osmMeasures.repository;

import org.apache.commons.lang3.tuple.Pair;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

public class MeasureTagCompleteness extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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

        return Cast.result(Index.map(mapReducer
                .osmType(OSMType.WAY)
                .mapPair(x -> {
                        // Get tags from key-value pairs
                        if (p.getOSMTag("key1", "value1") instanceof OSMTag) {
                            return x.getEntity().hasTagValue(tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key1", "value1")).getKey(),
                                tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key1", "value1")).getValue()) ? 1. : 0.;
                        } else if (p.getOSMTag("key1", "value1") instanceof OSMTagKey) {
                            return x.getEntity().hasTagKey(tagTranslator.getOSHDBTagKeyOf((OSMTagKey) p.getOSMTag("key1", "value1"))) ? 1. : 0.;
                        } else {
                            return 0.;
                        }
                    },
                    x -> {
                        // Get tags from key-value pairs
                        if (p.getOSMTag("key2", "value2") instanceof OSMTag) {
                            return x.getEntity().hasTagValue(tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key2", "value2")).getKey(),
                                tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key2", "value2")).getValue()) ? 1. : 0.;
                        } else if (p.getOSMTag("key2", "value2") instanceof OSMTagKey) {
                            return x.getEntity().hasTagKey(tagTranslator.getOSHDBTagKeyOf((OSMTagKey) p.getOSMTag("key2", "value2"))) ? 1. : 0.;
                        } else {
                            return 0.;
                        }
                    })
            .filter(x -> !x.getRight().equals(0.))
                .reduce(new IdentitySupplier(), new Accumulator(), new Combiner()),
            x -> {
                if (x.getRight().equals(0.) || x.getRight().isInfinite() || x.getRight().isNaN()) return -1.;
                Double ratio = (x.getLeft() / x.getRight()) * 100.;
                if (ratio.isNaN()) {
                    return -1.;
                } else if (ratio.isInfinite()) {
                    return -1.;
                } else {
                    return ratio;
                }
            }));
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
