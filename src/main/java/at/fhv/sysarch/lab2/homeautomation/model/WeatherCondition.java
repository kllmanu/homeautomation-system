package at.fhv.sysarch.lab2.homeautomation.model;

import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;

public record WeatherCondition(WeatherEnvironment.Weather value, String unit) {
    public WeatherCondition {
        if (value == null) throw new IllegalArgumentException("Weather value cannot be null");
        if (unit == null || unit.isBlank()) throw new IllegalArgumentException("Unit cannot be null or blank");
    }
}
