package org.giscience.osmMeasures.repository;

import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

import java.util.SortedMap;

public class MeasureBuildingAddressCompleteness extends MeasureAttributeCompleteness {

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
    public String defaultBasetags() {
        return "building";
    }

    @Override
    public String defaultSubtags() {
        return "addr:city;addr:street;addr:housenumber";
    }

    @Override
    public Integer defaultSubAll() {
        return 1;
    }

    @Override
    public Integer defaultBaseAll() {
        return 0;
    }

    @Override
    public String defaultReduceType() {
        return "COUNT";
    }

    @Override
    public String defaultOSMtype() {
        return "WAY";
    }

}
