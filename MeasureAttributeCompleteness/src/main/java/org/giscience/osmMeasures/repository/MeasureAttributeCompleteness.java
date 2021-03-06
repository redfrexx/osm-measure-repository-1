package org.giscience.osmMeasures.repository;

import com.vividsolutions.jts.geom.Polygonal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
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

    public String defaultSubtags() {
        return "";
    }

    public Integer defaultSubAll() {
        return 99;
    }

    public String defaultBasetags() {
        return "";
    }

    public Integer defaultBaseAll() {
        return 99;
    }

    public String defaultReduceType() {
        return "";
    }

    public String defaultOSMtype() {
        return "";
    }

    public enum MatchType {
        MATCHES1, MATCHES2, MATCHESBOTH, MATCHESNONE
    }

    @Override
    public SortedMap<GridCell, Number> compute(MapAggregator<GridCell, OSMEntitySnapshot> mapReducer, OSHDBRequestParameter p) throws Exception {
      List<List<String>> baseTags;
      List<List<String>> subTags;
      boolean baseAll;
      boolean subAll;
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

      // All or any tag
      if (defaultBaseAll().equals(99)) {
          baseAll = p.get("baseAll").toBoolean();
      } else {
          baseAll = defaultBaseAll() == 1;
      }
      if (defaultSubAll().equals(99)) {
          subAll = p.get("subAll").toBoolean();
      } else {
          subAll = defaultSubAll() == 1;
      }

      // Get parameters
      if (defaultReduceType().isEmpty()) {
          reduceType = p.get("reduceType").toString().toUpperCase();
      } else {
          reduceType = defaultReduceType();
      }
      if (defaultOSMtype().isEmpty()) {
          osmType = p.get("OSMtype").toString().toUpperCase();
      } else {
          osmType = defaultOSMtype();
      }

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
              OSMEntity entity = f.getEntity();
              boolean matches1;
              boolean matches2;
              // Sub Class
              if (subAll)
                  matches1 = has_all_tags(entity, subTags, tagTranslator);
              else
                  matches1 = has_any_tag(entity, subTags, tagTranslator);
              // Base class:
              if (baseAll)
                  matches2 = has_all_tags(entity, baseTags, tagTranslator);
              else
                  matches2 = has_any_tag(entity, baseTags, tagTranslator);

              if (matches1 && matches2)
                  return MatchType.MATCHESBOTH;
              else if (matches1)
                  return MatchType.MATCHES1;
              else if (matches2)
                  return MatchType.MATCHES2;
              else
                  return MatchType.MATCHESNONE;
          }, zerofill);


      return Cast.result(Index.reduce(
          compute_result(mapReducer2, reduceType),
          x -> {
              try {
                  Double totalRoadLength = (x.get(MatchType.MATCHES2).doubleValue() + x
                      .get(MatchType.MATCHESBOTH).doubleValue());
                  if (totalRoadLength > 0.) {
                      return (x.get(MatchType.MATCHESBOTH).doubleValue() / totalRoadLength)
                          * 100.;
                  } else {
                      return 0.;
                  }
              } catch (Exception e) {
                  System.out.println(" ------------------ ERROR --------------------- ");
                  System.out.println(e);
                  return 0.0;
              }
          }
      ));
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

  private boolean has_any_tag(OSMEntity entity, List<List<String>> tags,
      TagTranslator tagTranslator) {

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
              return false;
          }
      }
      return false;
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


  private SortedMap<OSHDBCombinedIndex<GridCell, MatchType>, ? extends Number> compute_result(
      MapAggregator<OSHDBCombinedIndex<GridCell, MatchType>, OSMEntitySnapshot> mapReducer,
      String reduceType)
      throws Exception {

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

  }

}
