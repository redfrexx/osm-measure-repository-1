package org.giscience.osmMeasures.repository;

import java.io.FileReader;
import java.util.ArrayList;
import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.json.simple.parser.JSONParser;

public class MeasureNumberOfPois extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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
        // EXAMPLE ONLY - PLEASE INSERT CODE HERE
      String POI_FILE = "./src/main/resources/extracted_pois.json";

        // Read in POIs from file
        JSONParser parser = new JSONParser();
        ArrayList pois = (ArrayList) parser.parse(new FileReader(POI_FILE));

        // Connect to database and create tagTranslator
        OSHDBJdbc oshdb = (OSHDBJdbc) this.getOSHDB();
        TagTranslator tagTranslator = new TagTranslator(oshdb.getConnection());

        return Cast.result(mapReducer
            .osmEntityFilter(entity -> {
                for (int i=0; i < pois.size(); i+=1) {
                    ArrayList elem = (ArrayList) pois.get(i);
                    OSHDBTag tag = tagTranslator.getOSHDBTagOf(elem.get(0).toString(), elem.get(1).toString());
                    if (entity.hasTagValue(tag.getKey(), tag.getValue())) return true;
                }
                return false;
            })
            .count());
    }
}
