package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.model.Temperature;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

/**
 * Note: This is an incomplete demonstration how a temperature could be implemented.
 * You may (actually, you should) change the logic so that it fits into your own actor system.
 * This class only acts as a demonstration for you to see, how an actor in java && pekko is structured.
 */
public class AirCondition extends AbstractBehavior<AirCondition.AirConditionCommand> {

    public static final ServiceKey<AirConditionCommand> SERVICE_KEY = ServiceKey.create(AirConditionCommand.class, "AirCondition");

    // commands our actor is able to receive
    public interface AirConditionCommand { }
    public record PowerAirCondition(boolean value) implements AirConditionCommand { }
    public record EnrichedTemperature(Temperature temperature) implements AirConditionCommand { }
    public record Subscribe(ActorRef<AirConditionStateChanged> subscriber) implements AirConditionCommand { }
    public record AirConditionStateChanged(boolean poweredOn) { }
    public record ReadState(ActorRef<AirConditionStateChanged> replyTo) implements AirConditionCommand { }

    // factory function called when a new instance of this actor is created
    public static Behavior<AirConditionCommand> create(String identifier) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, context.getSelf()));
            return new AirCondition(context, identifier);
        });
    }

    // mutable/immutable state variables of the actor defined here.
    private final String identifier;
    private boolean poweredOn = false;
    private ActorRef<AirConditionStateChanged> subscriber;

    // constructor initializing the actor
    public AirCondition(ActorContext<AirConditionCommand> context, String identifier) {
        super(context);
        this.identifier = identifier;
        getContext().getLog().info("AirCondition started");
    }

    // message handling logic = router for incoming messages and which callbacks should process them
    @Override
    public Receive<AirConditionCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(PowerAirCondition.class, this::onPowerAirCondition)
                .onMessage(EnrichedTemperature.class, this::onReadTemperature)
                .onMessage(Subscribe.class, this::onSubscribe)
                .onMessage(ReadState.class, this::onReadState)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<AirConditionCommand> onReadState(ReadState r) {
        r.replyTo().tell(new AirConditionStateChanged(this.poweredOn));
        return this;
    }

    private Behavior<AirConditionCommand> onSubscribe(Subscribe s) {
        this.subscriber = s.subscriber();
        this.subscriber.tell(new AirConditionStateChanged(poweredOn));
        return this;
    }

    private void notifySubscriber() {
        if (this.subscriber != null) {
            this.subscriber.tell(new AirConditionStateChanged(poweredOn));
        }
    }

    private Behavior<AirConditionCommand> onPowerAirCondition(PowerAirCondition p) {
        getContext().getLog().info("Aircondition power set to {}", p.value());
        this.poweredOn = p.value();
        notifySubscriber();
        return Behaviors.same();
    }

    private Behavior<AirConditionCommand> onReadTemperature(EnrichedTemperature r) {
        getContext().getLog().info("Aircondition reading {} {}", r.temperature().value(), r.temperature().unit());

        boolean previousState = poweredOn;
        if (r.temperature().value() > 20 && !poweredOn) {
            getContext().getLog().info("Aircondition turned ON");
            poweredOn = true;
        } else if (r.temperature().value() < 20 && poweredOn) {
            getContext().getLog().info("Aircondition turned OFF");
            poweredOn = false;
        }

        if (previousState != poweredOn) {
            notifySubscriber();
        }

        return Behaviors.same();
    }

    private AirCondition onPostStop() {
        getContext().getLog().info("AirCondition actor {}-{} stopped", identifier);
        return this;
    }
}
