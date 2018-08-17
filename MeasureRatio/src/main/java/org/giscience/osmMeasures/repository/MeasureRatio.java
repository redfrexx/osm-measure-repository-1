package org.giscience.osmMeasures.repository;

import java.util.EnumSet;
import org.apache.commons.lang3.tuple.Pair;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

public class MeasureRatio extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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
    public enum MatchType {
        MATCHES1, MATCHES2, MATCHESBOTH, MATCHESNONE
    }


    @Override
    public SortedMap<GridCell, Number> compute(MapAggregator<GridCell, OSMEntitySnapshot> mapReducer, OSHDBRequestParameter p) throws Exception {

        // Connect to database and create tagTranslator
        OSHDBJdbc oshdb = (OSHDBJdbc) this.getOSHDB();
        TagTranslator tagTranslator = new TagTranslator(oshdb.getConnection());

        // Create healthcare tag key
        OSHDBTag healthcareTag = tagTranslator.getOSHDBTagOf(p.get("key1").toString(), p.get("value1").toString());
        OSHDBTagKey buildingTagKey = tagTranslator.getOSHDBTagKeyOf(p.get("key2").toString());

        return Cast.result(Index.reduce(mapReducer
            .osmType(OSMType.WAY)
            .aggregateBy(f -> {
                OSMEntity entity = f.getEntity();
                boolean matches1 = entity.hasTagValue(healthcareTag.getKey(), healthcareTag.getValue());
                boolean matches2 = entity.hasTagKey(buildingTagKey.toInt());
                if (matches1 && matches2)
                    return MatchType.MATCHESBOTH;
                else if (matches1)
                    return MatchType.MATCHES1;
                else if (matches2)
                    return MatchType.MATCHES2;
                else
                    return MatchType.MATCHESNONE;
            })
            //.sum((SerializableFunction<OSMEntitySnapshot, Number>) x -> Geo.lengthOf(x.getGeometryUnclipped())),
            .count(),
            x -> {
                if (x.get(MatchType.MATCHES2).doubleValue() > 0.) {
                    return (x.get(MatchType.MATCHESBOTH).doubleValue() / x.get(MatchType.MATCHES2).doubleValue()) * 100.;
                } else {
                    return 0.;
                }
        }
            /*
            x -> {
            if (x.getRight().equals(0.) || x.getRight().isInfinite() || x.getRight().isNaN()) return -1.;
            Double ratio = (x.getLeft() / x.getRight()) * 100.;
            if (ratio.isNaN()) {
                return -1.;
            } else if (ratio.isInfinite()) {
                return -1.;
            } else {
                return ratio;
            }}*/
        ));
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
