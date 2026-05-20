package at.fhv.sysarch.lab2.homeautomation;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.environment.EnvironmentManager;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.room.Space;
import at.fhv.sysarch.lab2.homeautomation.uihandler.DemoHttpServer;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class HomeAutomationController extends AbstractBehavior<HomeAutomationController.ControllerCommand> {

    public interface ControllerCommand {}
    private record AirConditionListing(Receptionist.Listing listing) implements ControllerCommand {}
    private record BlindsListing(Receptionist.Listing listing) implements ControllerCommand {}
    private record MediaStationListing(Receptionist.Listing listing) implements ControllerCommand {}
    private record TempEnvListing(Receptionist.Listing listing) implements ControllerCommand {}
    private record WeatherEnvListing(Receptionist.Listing listing) implements ControllerCommand {}
    private enum Shutdown implements ControllerCommand { INSTANCE }

    private final String[] args;
    private ActorRef<AirCondition.AirConditionCommand> airCondition;
    private ActorRef<Blinds.BlindsCommand> blinds;
    private ActorRef<MediaStation.MediaStationCommand> mediaStation;
    private ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> tempEnv;
    private ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnv;
    private CompletionStage<ServerBinding> serverBinding;

    private boolean external = false;
    private boolean manual = false;

    public static Behavior<ControllerCommand> create(String[] args) {
        return Behaviors.setup(context -> new HomeAutomationController(context, args));
    }

    private HomeAutomationController(ActorContext<ControllerCommand> context, String[] args) {
        super(context);
        this.args = args;

        for (String arg : args) {
            getContext().getLog().info("Received argument: [{}]", arg);
            if (arg.trim().equals("--external")) {
                external = true;
            } else if (arg.trim().equals("--manual")) {
                manual = true;
            }
        }

        // Spawn EnvironmentManager
        getContext().spawn(EnvironmentManager.create(args), "environmentManager");

        // Spawn Space (which spawns Room, which spawns devices/sensors)
        getContext().spawn(Space.create("MainFloor"), "mainFloor");

        // Use Receptionist to find actors for the UI using proxy actors to avoid messageAdapter overwriting
        ActorRef<ControllerCommand> self = getContext().getSelf();

        getContext().getSystem().receptionist().tell(Receptionist.subscribe(AirCondition.SERVICE_KEY, 
                getContext().spawn(Behaviors.receiveMessage(listing -> {
                    self.tell(new AirConditionListing(listing));
                    return Behaviors.same();
                }), "acListingProxy")));
        
        getContext().getSystem().receptionist().tell(Receptionist.subscribe(Blinds.SERVICE_KEY, 
                getContext().spawn(Behaviors.receiveMessage(listing -> {
                    self.tell(new BlindsListing(listing));
                    return Behaviors.same();
                }), "blindsListingProxy")));
        
        getContext().getSystem().receptionist().tell(Receptionist.subscribe(MediaStation.SERVICE_KEY, 
                getContext().spawn(Behaviors.receiveMessage(listing -> {
                    self.tell(new MediaStationListing(listing));
                    return Behaviors.same();
                }), "mediaStationListingProxy")));
        
        getContext().getSystem().receptionist().tell(Receptionist.subscribe(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY, 
                getContext().spawn(Behaviors.receiveMessage(listing -> {
                    self.tell(new TempEnvListing(listing));
                    return Behaviors.same();
                }), "tempEnvListingProxy")));
        
        getContext().getSystem().receptionist().tell(Receptionist.subscribe(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY, 
                getContext().spawn(Behaviors.receiveMessage(listing -> {
                    self.tell(new WeatherEnvListing(listing));
                    return Behaviors.same();
                }), "weatherEnvListingProxy")));
    }

    @Override
    public Receive<ControllerCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(AirConditionListing.class, m -> onAirConditionListing(m.listing))
                .onMessage(BlindsListing.class, m -> onBlindsListing(m.listing))
                .onMessage(MediaStationListing.class, m -> onMediaStationListing(m.listing))
                .onMessage(TempEnvListing.class, m -> onTempEnvListing(m.listing))
                .onMessage(WeatherEnvListing.class, m -> onWeatherEnvListing(m.listing))
                .onMessage(Shutdown.class, m -> onShutdown())
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<ControllerCommand> onShutdown() {
        getContext().getLog().info("Shutting down...");
        if (serverBinding != null) {
            serverBinding
                    .thenCompose(ServerBinding::unbind)
                    .thenAccept(unbound -> getContext().getSystem().terminate());
        } else {
            getContext().getSystem().terminate();
        }
        return Behaviors.stopped();
    }

    private Behavior<ControllerCommand> onAirConditionListing(Receptionist.Listing listing) {
        this.airCondition = listing.getServiceInstances(AirCondition.SERVICE_KEY).stream().findFirst().orElse(null);
        checkIfAllReady();
        return this;
    }

    private Behavior<ControllerCommand> onBlindsListing(Receptionist.Listing listing) {
        this.blinds = listing.getServiceInstances(Blinds.SERVICE_KEY).stream().findFirst().orElse(null);
        checkIfAllReady();
        return this;
    }

    private Behavior<ControllerCommand> onMediaStationListing(Receptionist.Listing listing) {
        this.mediaStation = listing.getServiceInstances(MediaStation.SERVICE_KEY).stream().findFirst().orElse(null);
        checkIfAllReady();
        return this;
    }

    private Behavior<ControllerCommand> onTempEnvListing(Receptionist.Listing listing) {
        this.tempEnv = listing.getServiceInstances(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY).stream().findFirst().orElse(null);
        checkIfAllReady();
        return this;
    }

    private Behavior<ControllerCommand> onWeatherEnvListing(Receptionist.Listing listing) {
        this.weatherEnv = listing.getServiceInstances(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY).stream().findFirst().orElse(null);
        checkIfAllReady();
        return this;
    }

    private boolean serverStarted = false;

    private void checkIfAllReady() {
        if (!serverStarted && airCondition != null && blinds != null && mediaStation != null && tempEnv != null && weatherEnv != null) {
            startServer();
            serverStarted = true;
        }
    }

    private void startServer() {
        final Http http = Http.get(getContext().getSystem());
        String modeString = external ? "EXTERNAL" : (manual ? "MANUAL" : "INTERNAL");
        
        DemoHttpServer app = new DemoHttpServer(tempEnv, weatherEnv, airCondition, mediaStation, blinds, modeString, (ActorContext) getContext());
        this.serverBinding = http.newServerAt("localhost", 8084).bind(app.createRoute(getContext().getSystem()));

        getContext().getLog().info("Server started at http://localhost:8084/");
        getContext().getLog().info("PRESS RETURN TO EXIT");

        new Thread(() -> {
            try {
                System.in.read();
                getContext().getSelf().tell(Shutdown.INSTANCE);
            } catch (Exception e) {
                getContext().getLog().error("Error reading from stdin", e);
            }
        }).start();
    }

    private HomeAutomationController onPostStop() {
        getContext().getLog().info("HomeAutomation Application stopped");
        return this;
    }
}
