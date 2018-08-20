package org.giscience.osmMeasures.repository;

import com.vividsolutions.jts.geom.Polygonal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

public class MeasureHighwayNameCompleteness extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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

        String tag1String = p.get("tags1").toString();
        List<List<String>> tags1 = new ArrayList<>();
        Arrays.asList(tag1String.split(";")).forEach(x -> tags1.add(Arrays.asList(x.split("="))));

        String tag2String = p.get("tags2").toString();
        List<List<String>> tags2 = new ArrayList<>();
        Arrays.asList(tag2String.split(";")).forEach(x -> tags2.add(Arrays.asList(x.split("="))));

        // Get parameters
        String reduceType = p.get("reduceType").toString().toUpperCase();
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
                System.out.println("Invalid Option or non given.");
        }

        Collection<MatchType> zerofill = new LinkedList<>();
        zerofill.add(MatchType.MATCHES1);
        zerofill.add(MatchType.MATCHES2);
        zerofill.add(MatchType.MATCHESBOTH);
        zerofill.add(MatchType.MATCHESNONE);

        // Aggregate by attributes
        MapAggregator<OSHDBCombinedIndex<GridCell, MatchType>, OSMEntitySnapshot> mapReducer2 = mapReducer
            .aggregateBy(f -> {
                OSMEntity entity = f.getEntity();
                boolean matches1;
                boolean matches2;

                matches1 = hasTags(entity, tags1, tagTranslator);
                matches2 = hasTags(entity, tags2, tagTranslator);

                // Get tags from key-value pairs
                /*
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
                */

                if (matches1 && matches2)
                    return MatchType.MATCHESBOTH;
                else if (matches1)
                    return MatchType.MATCHES1;
                else if (matches2)
                    return MatchType.MATCHES2;
                else
                    return MatchType.MATCHESNONE;
            }, zerofill);

        // Reduce
        SortedMap<OSHDBCombinedIndex<GridCell, MatchType>, ? extends Number> result;
        switch (reduceType) {
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

    boolean hasTags(OSMEntity entity, List<List<String>> tags, TagTranslator tagTranslator) {

        for (List<String> elem : tags) {
            if (elem.size() == 1) {
                if (entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf(elem.get(0)))) {
                    return true;
                }
            } else if (elem.size() == 2) {
                OSHDBTag tag = tagTranslator.getOSHDBTagOf(elem.get(0), elem.get(1));
                if (entity.hasTagValue(tag.getKey(), tag.getValue())) {
                    return true;
                }
            } else {
                System.out.println("Invalid tag.");
            }
        }
        return false;
    }

    boolean hasAllTags(OSMEntity entity, List<List<String>> tags, TagTranslator tagTranslator) {

        for (List<String> elem : tags) {
            if (elem.size() == 1) {
                if (entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf(elem.get(0)))) {
                    return false;
                }
            } else if (elem.size() == 2) {
                OSHDBTag tag = tagTranslator.getOSHDBTagOf(elem.get(0), elem.get(1));
                if (entity.hasTagValue(tag.getKey(), tag.getValue())) {
                    return false;
                }
            } else {
                System.out.println("Invalid tag.");
            }
        }
        return true;
    }

}
