package at.fhv.sysarch.lab2.homeautomation.uihandler;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.fridge.FridgeModels;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.http.javadsl.model.sse.ServerSentEvent;
import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;

import java.time.Duration;

public class DashboardActor extends AbstractBehavior<DashboardActor.DashboardCommand> {
    public interface DashboardCommand {}

    //Webserver Request
    public record GetDashboardState(ActorRef<DashboardState> replyTo) implements DashboardCommand {}
    public record DashboardState(boolean acOn, boolean blindsClosed, boolean mediaPlaying, double temperature, String weather) {}

    //Internal Updates
    private record AirConditionUpdate(AirCondition.AirConditionStateChanged msg) implements DashboardCommand {}
    private record BlindsUpdate(Blinds.BlindsStateChanged msg) implements DashboardCommand {}
    private record MediaStationUpdate(MediaStation.MediaStationStateChanged msg) implements DashboardCommand {}
    private record TemperatureUpdate(TemperatureEnvironment.TemperatureResponse msg) implements DashboardCommand {}
    private record WeatherUpdate(WeatherEnvironment.WeatherResponse msg) implements DashboardCommand {}
    private record FridgeUpdate(FridgeModels.FridgeUpdate msg) implements DashboardCommand {}
    private enum Heartbeat implements DashboardCommand { INSTANCE }

    private final SourceQueueWithComplete<ServerSentEvent> sseQueue;

    //Cached State
    private boolean acOn = false;
    private boolean blindsClosed = false;
    private boolean mediaPlaying = false;
    private double currentTemp = 20.0;
    private String currentWeather = "SUNNY";

    public static Behavior<DashboardCommand> create(
            SourceQueueWithComplete<ServerSentEvent> sseQueue,
            ActorRef<AirCondition.AirConditionCommand> ac,
            ActorRef<Blinds.BlindsCommand> blinds,
            ActorRef<MediaStation.MediaStationCommand> media,
            ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temp,
            ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weather,
            ActorRef<FridgeModels.FridgeCommand> fridge) {
        return Behaviors.setup(context -> Behaviors.withTimers(timers -> 
            new DashboardActor(context, timers, sseQueue, ac, blinds, media, temp, weather, fridge)
        ));
    }

    private DashboardActor(ActorContext<DashboardCommand> context, TimerScheduler<DashboardCommand> timers,
                           SourceQueueWithComplete<ServerSentEvent> sseQueue,
                           ActorRef<AirCondition.AirConditionCommand> ac,
                           ActorRef<Blinds.BlindsCommand> blinds,
                           ActorRef<MediaStation.MediaStationCommand> media,
                           ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temp,
                           ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weather,
                           ActorRef<FridgeModels.FridgeCommand> fridge) {
        super(context);
        this.sseQueue = sseQueue;
        timers.startTimerAtFixedRate(Heartbeat.INSTANCE, Duration.ofSeconds(10));


        ac.tell(new AirCondition.Subscribe(context.messageAdapter(AirCondition.AirConditionStateChanged.class, AirConditionUpdate::new)));
        blinds.tell(new Blinds.Subscribe(context.messageAdapter(Blinds.BlindsStateChanged.class, BlindsUpdate::new)));
        media.tell(new MediaStation.Subscribe(context.messageAdapter(MediaStation.MediaStationStateChanged.class, MediaStationUpdate::new)));
        temp.tell(new TemperatureEnvironment.Subscribe(context.messageAdapter(TemperatureEnvironment.TemperatureResponse.class, TemperatureUpdate::new)));
        weather.tell(new WeatherEnvironment.Subscribe(context.messageAdapter(WeatherEnvironment.WeatherResponse.class, WeatherUpdate::new)));
        fridge.tell(new FridgeModels.Subscribe(context.messageAdapter(FridgeModels.FridgeUpdate.class, FridgeUpdate::new)));
    }

    @Override
    public Receive<DashboardCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetDashboardState.class, m -> {
                    m.replyTo.tell(new DashboardState(acOn, blindsClosed, mediaPlaying, currentTemp, currentWeather));
                    return this;
                })
                .onMessage(AirConditionUpdate.class, u -> {
                    this.acOn = u.msg.poweredOn();
                    sseQueue.offer(ServerSentEvent.create(this.acOn ? "ON" : "OFF", "aircondition"));
                    return this;
                })
                .onMessage(BlindsUpdate.class, u -> {
                    this.blindsClosed = u.msg.closed();
                    sseQueue.offer(ServerSentEvent.create(this.blindsClosed ? "CLOSED" : "OPEN", "blinds"));
                    return this;
                })
                .onMessage(MediaStationUpdate.class, u -> {
                    this.mediaPlaying = u.msg.playing();
                    sseQueue.offer(ServerSentEvent.create(this.mediaPlaying ? "PLAYING" : "IDLE", "mediastation"));
                    return this;
                })
                .onMessage(TemperatureUpdate.class, u -> {
                    this.currentTemp = u.msg.value();
                    sseQueue.offer(ServerSentEvent.create(String.format("%.1f", this.currentTemp), "temperature"));
                    return this;
                })
                .onMessage(WeatherUpdate.class, u -> {
                    this.currentWeather = u.msg.weather().toString();
                    sseQueue.offer(ServerSentEvent.create(this.currentWeather, "weather"));
                    return this;
                })
                .onMessage(FridgeUpdate.class, u -> {
                    sseQueue.offer(ServerSentEvent.create("update", "fridge-update"));
                    return this;
                })
                .onMessage(Heartbeat.class, h -> {
                    sseQueue.offer(ServerSentEvent.create("ping", "heartbeat"));
                    return this;
                })
                .build();
    }
}