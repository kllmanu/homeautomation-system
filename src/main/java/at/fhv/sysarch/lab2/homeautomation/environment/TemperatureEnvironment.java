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

public class TemperatureEnvironment extends AbstractBehavior<TemperatureEnvironment.TemperatureEnvironmentCommand> {

    public static final ServiceKey<TemperatureEnvironmentCommand> TEMPERATURE_ENVIRONMENT_SERVICE_KEY =
            ServiceKey.create(TemperatureEnvironmentCommand.class, "TemperatureEnvironment");

    public interface TemperatureEnvironmentCommand {}

    public record GetTemperature(ActorRef<TemperatureResponse> replyTo) implements TemperatureEnvironmentCommand {}

    public record SetTemperature(double value) implements TemperatureEnvironmentCommand {}

    public record SetTemperatureExternal(double value) implements TemperatureEnvironmentCommand {}

    public record Subscribe(ActorRef<TemperatureResponse> subscriber) implements TemperatureEnvironmentCommand {}

    public record TemperatureResponse(double value) {}

    private enum TemperatureTick implements TemperatureEnvironmentCommand {
        INSTANCE
    }

    private double temperature;
    private final List<ActorRef<TemperatureResponse>> subscribers = new ArrayList<>();

    public static Behavior<TemperatureEnvironmentCommand> create(double initialTemperature) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(TEMPERATURE_ENVIRONMENT_SERVICE_KEY, context.getSelf()));
            return Behaviors.withTimers(timers -> {
                timers.startTimerAtFixedRate(TemperatureTick.INSTANCE, Duration.ofSeconds(5));
                return new TemperatureEnvironment(context, initialTemperature);
            });
        });
    }

    private TemperatureEnvironment(ActorContext<TemperatureEnvironmentCommand> context, double initialTemperature) {
        super(context);
        this.temperature = initialTemperature;
        getContext().getLog().info("TemperatureEnvironment started (INTERNAL simulation) with {}°C", this.temperature);
    }

    @Override
    public Receive<TemperatureEnvironmentCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(TemperatureTick.class, tick -> onTemperatureTick())
                .onMessage(GetTemperature.class, this::onGetTemperature)
                .onMessage(SetTemperature.class, this::onSetTemperature)
                .onMessage(SetTemperatureExternal.class, this::onSetTemperatureExternal)
                .onMessage(Subscribe.class, this::onSubscribe)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<TemperatureEnvironmentCommand> onSubscribe(Subscribe s) {
        this.subscribers.add(s.subscriber());
        s.subscriber().tell(new TemperatureResponse(this.temperature));
        return this;
    }

    private void notifySubscribers() {
        TemperatureResponse res = new TemperatureResponse(this.temperature);
        for (ActorRef<TemperatureResponse> sub : subscribers) {
            sub.tell(res);
        }
    }

    private Behavior<TemperatureEnvironmentCommand> onGetTemperature(GetTemperature g) {
        g.replyTo().tell(new TemperatureResponse(this.temperature));
        return this;
    }

    private Behavior<TemperatureEnvironmentCommand> onSetTemperature(SetTemperature s) {
        this.temperature = s.value();
        getContext().getLog().info("TemperatureEnvironment manually set to {}°C", this.temperature);
        notifySubscribers();
        return this;
    }

    private Behavior<TemperatureEnvironmentCommand> onSetTemperatureExternal(SetTemperatureExternal s) {
        this.temperature = s.value();
        notifySubscribers();
        return this;
    }

    private Behavior<TemperatureEnvironmentCommand> onTemperatureTick() {
        // Simulate temperature change: random walk +/- 0.5 degrees
        double change = (Math.random() - 0.5);
        this.temperature += change;

        // Clamp temperature between -10 and +35
        if (this.temperature < -10) {
            this.temperature = -10;
        } else if (this.temperature > 35) {
            this.temperature = 35;
        }

        getContext().getLog().info("TemperatureEnvironment updated to {}°C", String.format("%.2f", this.temperature));
        notifySubscribers();
        return this;
    }

    private Behavior<TemperatureEnvironmentCommand> onPostStop() {
        getContext().getLog().info("TemperatureEnvironment stopped");
        return this;
    }
}
