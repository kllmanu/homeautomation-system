package at.fhv.sysarch.lab2.homeautomation.environment;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

import java.util.ArrayList;
import java.util.List;

public class ManualEnvironment extends AbstractBehavior<ManualEnvironment.ManualEnvironmentCommand> {

    public interface ManualEnvironmentCommand {}

    public record WrappedTemperatureCommand(TemperatureEnvironment.TemperatureEnvironmentCommand cmd) implements ManualEnvironmentCommand {}
    public record WrappedWeatherCommand(WeatherEnvironment.WeatherEnvironmentCommand cmd) implements ManualEnvironmentCommand {}

    private double temperature = 16.0;
    private WeatherEnvironment.Weather weather = WeatherEnvironment.Weather.SUNNY;
    private final List<ActorRef<TemperatureEnvironment.TemperatureResponse>> tempSubscribers = new ArrayList<>();
    private final List<ActorRef<WeatherEnvironment.WeatherResponse>> weatherSubscribers = new ArrayList<>();

    public static Behavior<ManualEnvironmentCommand> create() {
        return Behaviors.setup(ManualEnvironment::new);
    }

    private ManualEnvironment(ActorContext<ManualEnvironmentCommand> context) {
        super(context);

        // Register for Temperature
        ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> tempAdapter =
                context.messageAdapter(TemperatureEnvironment.TemperatureEnvironmentCommand.class, WrappedTemperatureCommand::new);
        context.getSystem().receptionist().tell(Receptionist.register(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY, tempAdapter));

        // Register for Weather
        ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherAdapter =
                context.messageAdapter(WeatherEnvironment.WeatherEnvironmentCommand.class, WrappedWeatherCommand::new);
        context.getSystem().receptionist().tell(Receptionist.register(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY, weatherAdapter));

        getContext().getLog().info("ManualEnvironment started (No simulation)");
    }

    @Override
    public Receive<ManualEnvironmentCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedTemperatureCommand.class, w -> onTemperatureCommand(w.cmd))
                .onMessage(WrappedWeatherCommand.class, w -> onWeatherCommand(w.cmd))
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<ManualEnvironmentCommand> onTemperatureCommand(TemperatureEnvironment.TemperatureEnvironmentCommand cmd) {
        if (cmd instanceof TemperatureEnvironment.GetTemperature g) {
            g.replyTo().tell(new TemperatureEnvironment.TemperatureResponse(this.temperature));
        } else if (cmd instanceof TemperatureEnvironment.SetTemperature s) {
            this.temperature = s.value();
            getContext().getLog().info("ManualEnvironment (Temperature) set to {}°C", this.temperature);
            notifyTempSubscribers();
        } else if (cmd instanceof TemperatureEnvironment.Subscribe s) {
            this.tempSubscribers.add(s.subscriber());
            s.subscriber().tell(new TemperatureEnvironment.TemperatureResponse(this.temperature));
        }
        return this;
    }

    private void notifyTempSubscribers() {
        TemperatureEnvironment.TemperatureResponse res = new TemperatureEnvironment.TemperatureResponse(this.temperature);
        for (ActorRef<TemperatureEnvironment.TemperatureResponse> sub : tempSubscribers) {
            sub.tell(res);
        }
    }

    private Behavior<ManualEnvironmentCommand> onWeatherCommand(WeatherEnvironment.WeatherEnvironmentCommand cmd) {
        if (cmd instanceof WeatherEnvironment.GetWeather g) {
            g.replyTo().tell(new WeatherEnvironment.WeatherResponse(this.weather));
        } else if (cmd instanceof WeatherEnvironment.SetWeather s) {
            this.weather = s.weather();
            getContext().getLog().info("ManualEnvironment (Weather) set to {}", this.weather);
            notifyWeatherSubscribers();
        } else if (cmd instanceof WeatherEnvironment.Subscribe s) {
            this.weatherSubscribers.add(s.subscriber());
            s.subscriber().tell(new WeatherEnvironment.WeatherResponse(this.weather));
        }
        return this;
    }

    private void notifyWeatherSubscribers() {
        WeatherEnvironment.WeatherResponse res = new WeatherEnvironment.WeatherResponse(this.weather);
        for (ActorRef<WeatherEnvironment.WeatherResponse> sub : weatherSubscribers) {
            sub.tell(res);
        }
    }

    private Behavior<ManualEnvironmentCommand> onPostStop() {
        getContext().getLog().info("ManualEnvironment stopped");
        return this;
    }
}
