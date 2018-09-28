package org.giscience.osmMeasures.repository;

import org.giscience.osmMeasures.repository.MeasureAttributeCompleteness;

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
    public String defaultBasetags() {
        return "highway=primary";
    }

    @Override
    public String defaultSubtags() {
        return "name;ref";
    }

    @Override
    public Integer defaultSubAll() {
        return 1;
    }

    @Override
    public Integer defaultBaseAll() {
        return 1;
    }

    @Override
    public String defaultReduceType() {
        return "LENGTH";
    }

    @Override
    public String defaultOSMtype() {
        return "WAY";
    }

}
