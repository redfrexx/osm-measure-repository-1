package org.giscience.osmMeasures.repository;

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
