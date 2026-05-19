package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.model.WeatherCondition;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

import java.time.Duration;

public class WeatherSensor extends AbstractBehavior<WeatherSensor.WeatherCommand> {

    public interface WeatherCommand { }

    private enum SenseWeather implements WeatherCommand {
        INSTANCE
    }

    private record WrappedListing(Receptionist.Listing listing) implements WeatherCommand { }

    private record WrappedWeatherResponse(WeatherEnvironment.WeatherResponse response) implements WeatherCommand { }

    public static Behavior<WeatherCommand> create(ActorRef<Blinds.BlindsCommand> blinds) {
        return Behaviors.setup(context ->
                Behaviors.withTimers(timers -> {
                    timers.startTimerAtFixedRate(SenseWeather.INSTANCE, Duration.ofSeconds(5));
                    return new WeatherSensor(context, blinds);
                })
        );
    }

    private final ActorRef<Blinds.BlindsCommand> blinds;
    private ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment;
    private WeatherEnvironment.Weather lastWeather = WeatherEnvironment.Weather.UNKNOWN;

    public WeatherSensor(ActorContext<WeatherCommand> context, ActorRef<Blinds.BlindsCommand> blinds) {
        super(context);
        this.blinds = blinds;

        // Subscribe to WeatherEnvironment listings
        ActorRef<Receptionist.Listing> listingResponseAdapter = getContext().messageAdapter(Receptionist.Listing.class, WrappedListing::new);
        getContext().getSystem().receptionist().tell(Receptionist.subscribe(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY, listingResponseAdapter));

        getContext().getLog().info("WeatherSensor started");
    }

    @Override
    public Receive<WeatherCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(SenseWeather.class, tick -> onSenseWeather())
                .onMessage(WrappedListing.class, w -> onListing(w.listing))
                .onMessage(WrappedWeatherResponse.class, w -> onWeatherResponse(w.response))
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<WeatherCommand> onSenseWeather() {
        if (this.weatherEnvironment != null) {
            getContext().ask(
                    WeatherEnvironment.WeatherResponse.class,
                    this.weatherEnvironment,
                    Duration.ofSeconds(3),
                    WeatherEnvironment.GetWeather::new,
                    (response, throwable) -> {
                        if (response != null) {
                            return new WrappedWeatherResponse(response);
                        } else {
                            getContext().getLog().error("Failed to measure weather: {}", throwable.getMessage());
                            return new WrappedWeatherResponse(new WeatherEnvironment.WeatherResponse(null));
                        }
                    }
            );
        } else {
            getContext().getLog().warn("WeatherSensor: WeatherEnvironment not available");
        }
        return this;
    }

    private Behavior<WeatherCommand> onListing(Receptionist.Listing listing) {
        this.weatherEnvironment = listing.getServiceInstances(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY)
                .stream()
                .findFirst()
                .orElse(null);
        return this;
    }

    private Behavior<WeatherCommand> onWeatherResponse(WeatherEnvironment.WeatherResponse response) {
        if (response.weather() != null && response.weather() != lastWeather && response.weather() != WeatherEnvironment.Weather.UNKNOWN) {
            getContext().getLog().info("WeatherSensor measured: {}", response.weather());
            this.lastWeather = response.weather();
            WeatherCondition wrapped = new WeatherCondition(response.weather(), "Condition");
            this.blinds.tell(new Blinds.WeatherChanged(wrapped));
        }
        return this;
    }

    private Behavior<WeatherCommand> onPostStop() {
        getContext().getLog().info("WeatherSensor stopped");
        return this;
    }
}
