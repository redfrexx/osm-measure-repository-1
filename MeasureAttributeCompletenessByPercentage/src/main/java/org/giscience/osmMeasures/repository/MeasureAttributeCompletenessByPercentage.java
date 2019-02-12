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

    public String default_subtags() {
        return "";
    }

    public String default_basetags() {
        return "";
    }

    public String default_reducetype() {
        return "";
    }

    public String default_osmtype() {
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
        if (default_basetags().isEmpty()) {
            baseTags = parse_tags(p.get("baseTags").toString());
        } else {
            baseTags = parse_tags(default_basetags());
        }
        if (default_subtags().isEmpty()) {
            subTags = parse_tags(p.get("subTags").toString());
        } else {
            subTags = parse_tags(default_subtags());
        }

        /**
        // Get reduce type
        if (default_reducetype().isEmpty()) {
            reduceType = p.get("reduceType").toString().toUpperCase();
        } else {
            reduceType = default_reducetype();
        }**/

        // Filter by OSM type
        if (default_osmtype().isEmpty()) {
            osmType = p.get("OSMtype").toString().toUpperCase();
        } else {
            osmType = default_osmtype();
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

        // Compute percentage of tag completeness for each feature
        return Cast.result(mapReducer
            .filter(x -> has_all_tags(x.getEntity(), baseTags, tagTranslator))
            //.map(x -> getTagCoverage(x.getEntity(), subTags, tagTranslator))
            .count());
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

    private static Double getTagCoverage(OSMEntity entity, List<List<String>> tags,
        TagTranslator tagTranslator) {

        Integer matches = 0;
        Integer invalidTags = 0;

        for (List<String> elem : tags) {
            if (elem.size() == 1) {
                if (entity.hasTagKey(tagTranslator.getOSHDBTagKeyOf(elem.get(0)))) {
                    matches += 1;
                }
            } else if (elem.size() == 2) {
                OSHDBTag tag = tagTranslator.getOSHDBTagOf(elem.get(0), elem.get(1));
                if (entity.hasTagValue(tag.getKey(), tag.getValue())) {
                    matches += 1;
                }
            } else {
                System.out.println("Invalid tag.");
                invalidTags += 1;
            }
        }
        return matches.doubleValue() / (tags.size() - invalidTags.doubleValue()) * 100.;
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
