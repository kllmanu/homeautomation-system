package at.fhv.sysarch.lab2.homeautomation.model;

public record Temperature(double value, String unit) {
    public Temperature {
        if (unit == null || unit.isBlank()) {
            throw new IllegalArgumentException("Unit cannot be null or blank");
        }
    }
}
