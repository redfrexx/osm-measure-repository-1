package org.giscience.osmMeasures.repository;

import com.vividsolutions.jts.geom.Polygonal;
import java.util.EnumSet;
import org.apache.commons.lang3.tuple.Pair;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
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
        OSHDBTagKey key1 = tagTranslator.getOSHDBTagKeyOf(p.get("key1").toString());
        OSHDBTagKey key2 = tagTranslator.getOSHDBTagKeyOf(p.get("key2").toString());
        String type = p.get("type").toString().toUpperCase();

        MapAggregator<OSHDBCombinedIndex<GridCell, MatchType>, OSMEntitySnapshot> mapReducer2 = mapReducer
            //.osmType(OSMType.WAY)
            .aggregateBy(f -> {
                OSMEntity entity = f.getEntity();
                boolean matches1 = entity.hasTagKey(key1.toInt());
                boolean matches2 = entity.hasTagKey(key2.toInt());
                if (matches1 && matches2)
                    return MatchType.MATCHESBOTH;
                else if (matches1)
                    return MatchType.MATCHES1;
                else if (matches2)
                    return MatchType.MATCHES2;
                else
                    return MatchType.MATCHESNONE;
            });

        SortedMap<OSHDBCombinedIndex<GridCell, MatchType>, ? extends Number> result;
        switch (type) {
            case "COUNT":
                result = mapReducer2.count();
                break;
            case "LENGTH":
                result = mapReducer2
                    .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
                        return Geo.lengthOf(snapshot.getGeometry());
                    });
                break;
            case "PERIMETER":
                result = mapReducer2
                    .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
                        if (snapshot.getGeometry() instanceof Polygonal)
                            return Geo.lengthOf(snapshot.getGeometry().getBoundary());
                        else
                            return 0.0;
                    });
                break;
            case "AREA":
                result = mapReducer2
                    .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
                        return Geo.areaOf(snapshot.getGeometry());
                    });
                break;
            default:
                result = null;
        }

        return Cast.result(Index.reduce(result,
            //.sum((SerializableFunction<OSMEntitySnapshot, Number>) x -> Geo.lengthOf(x.getGeometry())),
            //.count(),
            x -> {
            Double totalRoadLength = (x.get(MatchType.MATCHES2).doubleValue() + x.get(MatchType.MATCHESBOTH).doubleValue());
                if (totalRoadLength > 0.) {
                    return (x.get(MatchType.MATCHESBOTH).doubleValue() / totalRoadLength) * 100.;
                } else {
                    return 100.;
                }}
        ));
    }


}
