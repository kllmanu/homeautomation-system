package at.fhv.sysarch.lab2.homeautomation.fridge;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;

public class FridgeWeightSensor extends AbstractBehavior<FridgeWeightSensor.Command> {

    public interface Command {}
    private enum Tick implements Command { INSTANCE }
    private record InternalResponse(FridgeModels.WeightResponse res) implements Command {}

    private final ActorRef<FridgeModels.FridgeCommand> fridge;

    public static Behavior<Command> create(ActorRef<FridgeModels.FridgeCommand> fridge) {
        return Behaviors.setup(context -> Behaviors.withTimers(timers -> {
            timers.startTimerAtFixedRate(Tick.INSTANCE, Duration.ofSeconds(10));
            return new FridgeWeightSensor(context, fridge);
        }));
    }

    private FridgeWeightSensor(ActorContext<Command> context, ActorRef<FridgeModels.FridgeCommand> fridge) {
        super(context);
        this.fridge = fridge;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Tick.class, t -> onTick())
                .onMessage(InternalResponse.class, r -> onResponse(r.res))
                .build();
    }

    private Behavior<Command> onTick() {
        getContext().ask(FridgeModels.WeightResponse.class, fridge, Duration.ofSeconds(3), 
                FridgeModels.ReadWeight::new, (res, exc) -> {
            if (res != null) return new InternalResponse(res);
            else return new InternalResponse(new FridgeModels.WeightResponse(-1.0));
        });
        return this;
    }

    private Behavior<Command> onResponse(FridgeModels.WeightResponse r) {
        getContext().getLog().info("FridgeWeightSensor: Measured weight = {}kg", String.format("%.2f", r.weight()));
        return this;
    }
}
