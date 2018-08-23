package org.giscience.osmMeasures.repository;

import com.vividsolutions.jts.geom.Polygonal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import javafx.util.Pair;
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
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

public class MeasureAttributeCompleteness extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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

        // Parse tags
        List<List<String>> subTags = parseTags(p.get("subTags").toString());
        boolean subAll = p.get("subAll").toBoolean();

        List<List<String>> baseTags = parseTags(p.get("baseTags").toString());
        boolean baseAll = p.get("baseAll").toBoolean();

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

        // Zerofill collection that is passed to aggregateBy to make sure that all aggregated
        // elements are present in the result
        Collection<MatchType> zerofill = new LinkedList<>();
        zerofill.add(MatchType.MATCHES1);
        zerofill.add(MatchType.MATCHES2);
        zerofill.add(MatchType.MATCHESBOTH);
        zerofill.add(MatchType.MATCHESNONE);

        // Aggregate by attributes
        MapAggregator<OSHDBCombinedIndex<GridCell, MatchType>, OSMEntitySnapshot> mapReducer2 = mapReducer
            .aggregateBy(f -> {
                try {
                    OSMEntity entity = f.getEntity();
                    boolean matches1;
                    boolean matches2;
                    // Sub Class
                    if (subAll)
                        matches1 = hasAllTags(entity, subTags, tagTranslator);
                    else
                        matches1 = hasAnyTag(entity, subTags, tagTranslator);
                    // Base class:
                    if (baseAll)
                        matches2 = hasAllTags(entity, baseTags, tagTranslator);
                    else
                        matches2 = hasAnyTag(entity, baseTags, tagTranslator);

                    if (matches1 && matches2)
                        return MatchType.MATCHESBOTH;
                    else if (matches1)
                        return MatchType.MATCHES1;
                    else if (matches2)
                        return MatchType.MATCHES2;
                    else
                        return MatchType.MATCHESNONE;
                } catch (Exception e) {
                    System.out.println(" ------------------ ERROR --------------------- ");
                    System.out.println(e);
                    return MatchType.MATCHESNONE;
                }
            }, zerofill);


        SortedMap<OSHDBCombinedIndex<GridCell, MatchType>, ? extends Number> mapReducer3;
        try {
            mapReducer3 = computeResult(mapReducer2, reduceType);

            for (Entry entry : mapReducer3.entrySet()) {
                System.out.println(entry.getKey() + " - " + entry.getValue())
            }

        } catch(Exception e) {
            System.out.println(" ------------------ ERROR --------------------- ");
            System.out.println(e);
            return null;
        }

        return Cast.result(Index.reduce(mapReducer3,
            x -> {
                try {
                    Double totalRoadLength = (x.get(MatchType.MATCHES2).doubleValue() + x
                        .get(MatchType.MATCHESBOTH).doubleValue());
                    if (totalRoadLength > 0.) {
                        return (x.get(MatchType.MATCHESBOTH).doubleValue() / totalRoadLength)
                            * 100.;
                    } else {
                        return null;
                    }
                } catch (Exception e) {
                    System.out.println(" ------------------ ERROR --------------------- ");
                    System.out.println(e);
                    return null;
                }
            }
        ));

    }

    private boolean hasAnyTag(OSMEntity entity, List<List<String>> tags,
        TagTranslator tagTranslator) {

        for (List<String> elem : tags) {
            if (elem.size() == 1) {
                if (entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf(elem.get(0)))) {
                    return true;
                }
            } else if (elem.size() == 2) {
                OSHDBTag tag = tagTranslator.getOSHDBTagOf(elem.get(0), elem.get(1));
                if (entity.hasTagValue(tag.getKey(), tag.getValue()))   {
                    return true;
                }
            } else {
                System.out.println("Invalid tag.");
                return false;
            }
        }
        return false;
    }

    private boolean hasAllTags(OSMEntity entity, List<List<String>> tags,
        TagTranslator tagTranslator) {

        for (List<String> elem : tags) {
            if (elem.size() == 1) {
                if (!entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf(elem.get(0)))) {
                    return false;
                }
            } else if (elem.size() == 2) {
                OSHDBTag tag = tagTranslator.getOSHDBTagOf(elem.get(0), elem.get(1));
                if (!entity.hasTagValue(tag.getKey(), tag.getValue())) {
                    return false;
                }
            } else {
                System.out.println("Invalid tag.");
                return false;
            }
        }
        return true;
    }

    private List<List<String>> parseTags(String rawString) {
        List<List<String>> tags = new ArrayList<>();
        Arrays.asList(rawString.split(";")).forEach(x -> tags.add(Arrays.asList(x.split("="))));
        System.out.println(tags);
        return tags;
    }

    private SortedMap<OSHDBCombinedIndex<GridCell, MatchType>, ? extends Number> computeResult(
        MapAggregator<OSHDBCombinedIndex<GridCell, MatchType>, OSMEntitySnapshot> mapReducer,
        String reduceType)
        throws Exception {

        try {
            switch (reduceType) {

                case "COUNT":
                    return mapReducer.count();
                case "LENGTH":
                    return mapReducer
                        .sum((SerializableFunction<OSMEntitySnapshot, Number>)
                            snapshot -> Geo.lengthOf(snapshot.getGeometry()));
                case "PERIMETER":
                    return mapReducer
                        .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
                            if (snapshot.getGeometry() instanceof Polygonal)
                                return Geo.lengthOf(snapshot.getGeometry().getBoundary());
                            else
                                return 0.0;
                        });
                case "AREA":
                    return mapReducer
                        .sum((SerializableFunction<OSMEntitySnapshot, Number>)
                            snapshot -> Geo.areaOf(snapshot.getGeometry()));
                default:
                    return null;
            }
        } catch (Exception e) {
            System.out.println(" ------------------ ERROR --------------------- ");
            System.out.println(e);
            return null;
        }
    }

}
