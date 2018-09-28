package org.giscience.osmMeasures.repository;

public class MeasureSourceCompleteness extends MeasureAttributeCompleteness {

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
        return "";
    }

    @Override
    public String defaultSubtags() {
        return "source";
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
        return "COUNT";
    }

    @Override
    public String defaultOSMtype() {
        return "WAY";
    }

}
