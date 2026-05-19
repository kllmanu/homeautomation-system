package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.model.Temperature;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

import java.time.Duration;

public class TemperatureSensor extends AbstractBehavior<TemperatureSensor.TemperatureCommand> {

    public interface TemperatureCommand { }

    private enum SenseTemperature implements TemperatureCommand {
        INSTANCE
    }

    private record WrappedListing(Receptionist.Listing listing) implements TemperatureCommand { }

    private record WrappedTemperatureResponse(TemperatureEnvironment.TemperatureResponse response) implements TemperatureCommand { }

    public static Behavior<TemperatureCommand> create(ActorRef<AirCondition.AirConditionCommand> airCondition) {
        return Behaviors.setup(context ->
            Behaviors.withTimers(timers -> {
                timers.startTimerAtFixedRate(SenseTemperature.INSTANCE, Duration.ofSeconds(2));
                return new TemperatureSensor(context, airCondition);
            })
        );
    }

    private final ActorRef<AirCondition.AirConditionCommand> airCondition;
    private ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment;
    private double lastTemperature = Double.NaN;

    public TemperatureSensor(ActorContext<TemperatureCommand> context, ActorRef<AirCondition.AirConditionCommand> airCondition) {
        super(context);
        this.airCondition = airCondition;

        // Subscribe to TemperatureEnvironment listings
        ActorRef<Receptionist.Listing> listingResponseAdapter = getContext().messageAdapter(Receptionist.Listing.class, WrappedListing::new);
        getContext().getSystem().receptionist().tell(Receptionist.subscribe(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY, listingResponseAdapter));

        getContext().getLog().info("TemperatureSensor started");
    }

    @Override
    public Receive<TemperatureCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(SenseTemperature.class, tick -> onSenseTemperature())
                .onMessage(WrappedListing.class, w -> onListing(w.listing))
                .onMessage(WrappedTemperatureResponse.class, w -> onTemperatureResponse(w.response))
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<TemperatureCommand> onSenseTemperature() {
        if (this.temperatureEnvironment != null) {
            getContext().ask(
                    TemperatureEnvironment.TemperatureResponse.class,
                    this.temperatureEnvironment,
                    Duration.ofSeconds(3),
                    TemperatureEnvironment.GetTemperature::new,
                    (response, throwable) -> {
                        if (response != null) {
                            return new WrappedTemperatureResponse(response);
                        } else {
                            getContext().getLog().error("Failed to measure temperature: {}", throwable.getMessage());
                            return new WrappedTemperatureResponse(new TemperatureEnvironment.TemperatureResponse(Double.NaN));
                        }
                    }
            );
        } else {
            getContext().getLog().warn("TemperatureSensor: TemperatureEnvironment not available");
        }
        return this;
    }

    private Behavior<TemperatureCommand> onListing(Receptionist.Listing listing) {
        this.temperatureEnvironment = listing.getServiceInstances(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY)
                .stream()
                .findFirst()
                .orElse(null);
        return this;
    }

    private Behavior<TemperatureCommand> onTemperatureResponse(TemperatureEnvironment.TemperatureResponse response) {
        if (response.value() != lastTemperature && !Double.isNaN(response.value())) {
            getContext().getLog().info("TemperatureSensor measured: {}°C", response.value());
            this.lastTemperature = response.value();
            Temperature wrapped = new Temperature(response.value(), "Celsius");
            this.airCondition.tell(new AirCondition.EnrichedTemperature(wrapped));
        }
        return this;
    }

    private Behavior<TemperatureCommand> onPostStop() {
        getContext().getLog().info("TemperatureSensor stopped");
        return this;
    }
}
