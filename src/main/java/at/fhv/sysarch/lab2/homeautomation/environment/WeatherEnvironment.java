package at.fhv.sysarch.lab2.homeautomation.environment;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

import java.time.Duration;

public class WeatherEnvironment extends AbstractBehavior<WeatherEnvironment.WeatherEnvironmentCommand> {

    public static final ServiceKey<WeatherEnvironmentCommand> WEATHER_ENVIRONMENT_SERVICE_KEY =
            ServiceKey.create(WeatherEnvironmentCommand.class, "WeatherEnvironment");

    public enum Weather {
        SUNNY, CLOUDY, RAINY
    }

    public interface WeatherEnvironmentCommand {}

    public record GetWeather(ActorRef<WeatherResponse> replyTo) implements WeatherEnvironmentCommand {}

    public record SetWeather(Weather weather) implements WeatherEnvironmentCommand {}

    public record SetWeatherExternal(Weather weather) implements WeatherEnvironmentCommand {}

    public enum Mode {
        INTERNAL, EXTERNAL, MANUAL
    }

    public record WeatherResponse(Weather weather) {}

    private enum WeatherTick implements WeatherEnvironmentCommand {
        INSTANCE
    }

    private Weather weather;
    private final Mode mode;

    public static Behavior<WeatherEnvironmentCommand> create(Weather initialWeather, Mode mode) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(WEATHER_ENVIRONMENT_SERVICE_KEY, context.getSelf()));
            return Behaviors.withTimers(timers -> {
                if (mode == Mode.INTERNAL) {
                    timers.startTimerAtFixedRate(WeatherTick.INSTANCE, Duration.ofSeconds(10));
                }
                return new WeatherEnvironment(context, initialWeather, mode);
            });
        });
    }

    private WeatherEnvironment(ActorContext<WeatherEnvironmentCommand> context, Weather initialWeather, Mode mode) {
        super(context);
        this.weather = initialWeather;
        this.mode = mode;
        getContext().getLog().info("WeatherEnvironment started in {} mode with weather: {}", this.mode, this.weather);
    }

    @Override
    public Receive<WeatherEnvironmentCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WeatherTick.class, tick -> onWeatherTick())
                .onMessage(GetWeather.class, this::onGetWeather)
                .onMessage(SetWeather.class, this::onSetWeather)
                .onMessage(SetWeatherExternal.class, this::onSetWeatherExternal)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<WeatherEnvironmentCommand> onGetWeather(GetWeather g) {
        g.replyTo().tell(new WeatherResponse(this.weather));
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onSetWeather(SetWeather s) {
        this.weather = s.weather();
        getContext().getLog().info("WeatherEnvironment manually set to {}", this.weather);
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onSetWeatherExternal(SetWeatherExternal s) {
        if (this.mode == Mode.EXTERNAL) {
            this.weather = s.weather();
            getContext().getLog().info("WeatherEnvironment updated from EXTERNAL to {}", this.weather);
        }
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onWeatherTick() {
        if (this.mode == Mode.INTERNAL) {
            // Change weather randomly
            Weather[] weathers = Weather.values();
            this.weather = weathers[(int) (Math.random() * weathers.length)];
            getContext().getLog().info("WeatherEnvironment updated to weather: {}", this.weather);
        }
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onPostStop() {
        getContext().getLog().info("WeatherEnvironment stopped");
        return this;
    }
}
