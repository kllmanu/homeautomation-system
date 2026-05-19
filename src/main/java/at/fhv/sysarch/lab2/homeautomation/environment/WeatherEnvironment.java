package at.fhv.sysarch.lab2.homeautomation.environment;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;

public class WeatherEnvironment extends AbstractBehavior<WeatherEnvironment.WeatherEnvironmentCommand> {

    public enum Weather {
        SUNNY, CLOUDY, RAINY
    }

    public interface WeatherEnvironmentCommand {}

    private enum WeatherTick implements WeatherEnvironmentCommand {
        INSTANCE
    }

    private Weather weather;

    public static Behavior<WeatherEnvironmentCommand> create(Weather initialWeather) {
        return Behaviors.setup(context ->
            Behaviors.withTimers(timers -> {
                timers.startTimerAtFixedRate(WeatherTick.INSTANCE, Duration.ofSeconds(10));
                return new WeatherEnvironment(context, initialWeather);
            })
        );
    }

    private WeatherEnvironment(ActorContext<WeatherEnvironmentCommand> context, Weather initialWeather) {
        super(context);
        this.weather = initialWeather;
        getContext().getLog().info("WeatherEnvironment started with weather: {}", this.weather);
    }

    @Override
    public Receive<WeatherEnvironmentCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WeatherTick.class, tick -> onWeatherTick())
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<WeatherEnvironmentCommand> onWeatherTick() {
        // Change weather randomly
        Weather[] weathers = Weather.values();
        this.weather = weathers[(int) (Math.random() * weathers.length)];
        getContext().getLog().info("WeatherEnvironment updated to weather: {}", this.weather);
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onPostStop() {
        getContext().getLog().info("WeatherEnvironment stopped");
        return this;
    }
}
