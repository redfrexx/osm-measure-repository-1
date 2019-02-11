package org.giscience.osmMeasures.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

public class MeasureAttributeCompletenessByPercentage extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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


    public String defaultSubtags() {
        return "";
    }

    public String defaultBasetags() {
        return "";
    }

    public String defaultReduceType() {
        return "";
    }

    public String defaultOSMtype() {
        return "";
    }

    @Override
    public SortedMap<GridCell, Number> compute(MapAggregator<GridCell, OSMEntitySnapshot> mapReducer, OSHDBRequestParameter p) throws Exception {
        List<List<String>> baseTags;
        List<List<String>> subTags;
        String reduceType;
        String osmType;

        // Connect to database and create tagTranslator
        OSHDBJdbc oshdb = (OSHDBJdbc) this.getOSHDB();
        TagTranslator tagTranslator = new TagTranslator(oshdb.getConnection());

        // Check input parameters

        // Parse tags
        if (defaultBasetags().isEmpty()) {
            baseTags = parse_tags(p.get("baseTags").toString());
        } else {
            baseTags = parse_tags(defaultBasetags());
        }
        if (defaultSubtags().isEmpty()) {
            subTags = parse_tags(p.get("subTags").toString());
        } else {
            subTags = parse_tags(defaultSubtags());
        }

        // Get reduce type
        if (defaultReduceType().isEmpty()) {
            reduceType = p.get("reduceType").toString().toUpperCase();
        } else {
            reduceType = defaultReduceType();
        }

        // Filter by OSM type
        if (defaultOSMtype().isEmpty()) {
            osmType = p.get("OSMtype").toString().toUpperCase();
        } else {
            osmType = defaultOSMtype();
        }
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

        // Get all features that match the base tags
        mapReducer = mapReducer.filter(x -> has_all_tags(x.getEntity(), baseTags, tagTranslator));

        // Compute percentage of tag completeness for each feature
        return Cast.result(mapReducer
            .map(x -> compute_tag_completeness(x.getEntity(), subTags, tagTranslator))
            .average());
    }

    private double compute_tag_completeness(
        OSMEntity entity,
        List<List<String>> tags,
        TagTranslator tagTranslator) {

        Integer count = 0;
        for (List<String> tag : tags) {
            if (tag.size() == 1) {
                if (entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf(tag.get(0)))) {
                    count += 1;
                }
            } else if (tag.size() == 2) {
                OSHDBTag oshdbTag = tagTranslator.getOSHDBTagOf(tag.get(0), tag.get(1));
                if (entity.hasTagValue(oshdbTag.getKey(), oshdbTag.getValue())) {
                    count += 1;
                }
            }
        }
        return count / (double) tags.size();
    }

    private boolean has_all_tags(OSMEntity entity, List<List<String>> tags,
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

    private List<List<String>> parse_tags(String rawString) {
        List<List<String>> tags = new ArrayList<>();
        Arrays.asList(rawString.split(";")).forEach(x -> tags.add(Arrays.asList(x.split("="))));
        System.out.println(tags);
        return tags;
    }

}
