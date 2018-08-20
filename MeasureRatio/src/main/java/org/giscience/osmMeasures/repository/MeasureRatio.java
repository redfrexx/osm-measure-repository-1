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

        // Get parameters
        //OSHDBTagKey key1 = tagTranslator.getOSHDBTagKeyOf(p.get("key1").toString());
        //OSHDBTagKey key2 = tagTranslator.getOSHDBTagKeyOf(p.get("key2").toString());

        String aggregationType = p.get("aggregationType").toString().toUpperCase();
        String osmType = p.get("osmType").toString().toUpperCase();

        // Filter by OSM type
        switch (osmType) {
            case "WAY":
                mapReducer = mapReducer.osmType(OSMType.WAY);
                break;
            case "NODE":
                mapReducer = mapReducer.osmType(OSMType.NODE);
                break;
            case "RELATION":
                mapReducer = mapReducer.osmType(OSMType.RELATION);
                break;
            default:
                System.out.println("Invalid Option");
        }

        // Aggregate by attributes
        MapAggregator<OSHDBCombinedIndex<GridCell, MatchType>, OSMEntitySnapshot> mapReducer2 = mapReducer
            .aggregateBy(f -> {
                OSMEntity entity = f.getEntity();
                boolean matches1;
                boolean matches2;

                // Get tags from key-value pairs
                if (p.getOSMTag("key1", "value1") instanceof OSMTag) {
                    matches1 = entity.hasTagValue(tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key1", "value1")).getKey(),
                        tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key1", "value1")).getValue());
                } else if (p.getOSMTag("key1", "value1") instanceof OSMTagKey) {
                    matches1 = entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf((OSMTagKey) p.getOSMTag("key1", "value1")));
                } else {
                    matches1 = false;
                }

                // Get tags from key-value pairs
                if (p.getOSMTag("key2", "value2") instanceof OSMTag) {
                    matches2 = entity.hasTagValue(tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key2", "value2")).getKey(),
                        tagTranslator.getOSHDBTagOf((OSMTag) p.getOSMTag("key2", "value2")).getValue());
                } else if (p.getOSMTag("key2", "value2") instanceof OSMTagKey) {
                    matches2 = entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf((OSMTagKey) p.getOSMTag("key2", "value2")));
                } else {
                    matches2 = false;
                }

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
        switch (aggregationType) {
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
