package at.fhv.sysarch.lab2.homeautomation.environment;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class WeatherEnvironment extends AbstractBehavior<WeatherEnvironment.WeatherEnvironmentCommand> {

    public static final ServiceKey<WeatherEnvironmentCommand> WEATHER_ENVIRONMENT_SERVICE_KEY =
            ServiceKey.create(WeatherEnvironmentCommand.class, "WeatherEnvironment");

    public enum Weather {
        SUNNY, CLOUDY, RAINY, UNKNOWN
    }

    public interface WeatherEnvironmentCommand {}

    public record GetWeather(ActorRef<WeatherResponse> replyTo) implements WeatherEnvironmentCommand {}

    public record SetWeather(Weather weather) implements WeatherEnvironmentCommand {}

    public record SetWeatherExternal(Weather weather) implements WeatherEnvironmentCommand {}

    public record Subscribe(ActorRef<WeatherResponse> subscriber) implements WeatherEnvironmentCommand {}

    public record WeatherResponse(Weather weather) {}

    private enum WeatherTick implements WeatherEnvironmentCommand {
        INSTANCE
    }

    private Weather weather;
    private final List<ActorRef<WeatherResponse>> subscribers = new ArrayList<>();

    public static Behavior<WeatherEnvironmentCommand> create(Weather initialWeather) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(WEATHER_ENVIRONMENT_SERVICE_KEY, context.getSelf()));
            return Behaviors.withTimers(timers -> {
                timers.startTimerAtFixedRate(WeatherTick.INSTANCE, Duration.ofSeconds(10));
                return new WeatherEnvironment(context, initialWeather);
            });
        });
    }

    private WeatherEnvironment(ActorContext<WeatherEnvironmentCommand> context, Weather initialWeather) {
        super(context);
        this.weather = initialWeather;
        getContext().getLog().info("WeatherEnvironment started (INTERNAL simulation) with weather: {}", this.weather);
    }

    @Override
    public Receive<WeatherEnvironmentCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WeatherTick.class, tick -> onWeatherTick())
                .onMessage(GetWeather.class, this::onGetWeather)
                .onMessage(SetWeather.class, this::onSetWeather)
                .onMessage(SetWeatherExternal.class, this::onSetWeatherExternal)
                .onMessage(Subscribe.class, this::onSubscribe)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<WeatherEnvironmentCommand> onSubscribe(Subscribe s) {
        this.subscribers.add(s.subscriber());
        s.subscriber().tell(new WeatherResponse(this.weather));
        return this;
    }

    private void notifySubscribers() {
        WeatherResponse res = new WeatherResponse(this.weather);
        for (ActorRef<WeatherResponse> sub : subscribers) {
            sub.tell(res);
        }
    }

    private Behavior<WeatherEnvironmentCommand> onGetWeather(GetWeather g) {
        g.replyTo().tell(new WeatherResponse(this.weather));
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onSetWeather(SetWeather s) {
        this.weather = s.weather();
        getContext().getLog().info("WeatherEnvironment manually set to {}", this.weather);
        notifySubscribers();
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onSetWeatherExternal(SetWeatherExternal s) {
        this.weather = s.weather();
        notifySubscribers();
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onWeatherTick() {
        // Change weather randomly
        Weather[] weathers = Weather.values();
        this.weather = weathers[(int) (Math.random() * (weathers.length - 1))]; // Exclude UNKNOWN
        getContext().getLog().info("WeatherEnvironment updated to weather: {}", this.weather);
        notifySubscribers();
        return this;
    }

    private Behavior<WeatherEnvironmentCommand> onPostStop() {
        getContext().getLog().info("WeatherEnvironment stopped");
        return this;
    }
}
