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
        return "highway";
    }

    @Override
    public String defaultSubtags() {
        return "name=Hisingsleden";
    }

    @Override
    public Integer defaultSubAll() {
        return 0;
    }

    @Override
    public Integer defaultBaseAll() {
        return 0;
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
