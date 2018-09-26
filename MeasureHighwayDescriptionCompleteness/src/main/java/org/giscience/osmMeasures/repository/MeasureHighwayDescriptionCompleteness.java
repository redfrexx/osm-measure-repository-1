package org.giscience.osmMeasures.repository;

import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.giscience.osmMeasures.repository.MeasureAttributeCompleteness;

import java.util.SortedMap;

public class MeasureHighwayDescriptionCompleteness extends MeasureAttributeCompleteness {

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
    public String basetags() {
        return "highway";
    }

    @Override
    public String subtags() {
        return "name=Hisingsleden";
    }

    @Override
    public Integer baseAll() {
        return 0;
    }

    @Override
    public Integer subAll() {
        return 0;
    }

    @Override
    public String reduceType() {
        return "LENGTH";
    }

    @Override
    public String osmType() {
        return "WAY";
    }

}
