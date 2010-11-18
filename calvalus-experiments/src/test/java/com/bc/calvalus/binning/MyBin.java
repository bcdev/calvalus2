package com.bc.calvalus.binning;


class MyBin extends AbstractBin<MyObservation> {
    int numObservations;
    double sumX;
    double sumXX;
    double weight;

    public MyBin(int binIndex) {
        super(binIndex);
    }

    @Override
    public void addObservation(MyObservation observation) {
        double x = Math.log(observation.getX());
        sumX += x;
        sumXX += x * x;
        numObservations++;
    }

    @Override
    public void addBin(Bin bin) {
        // todo - get rid of ugly cast
        addBin((MyBin) bin);
    }

    public void addBin(MyBin bin) {
        sumX += bin.sumX;
        sumXX += bin.sumXX;
        numObservations += bin.numObservations;
        weight += bin.weight;
    }

    @Override
    public void close() {
        weight = Math.sqrt(numObservations);
        sumX /= weight;
        sumXX /= weight;
    }

    public int getNumObservations() {
        return numObservations;
    }
}
