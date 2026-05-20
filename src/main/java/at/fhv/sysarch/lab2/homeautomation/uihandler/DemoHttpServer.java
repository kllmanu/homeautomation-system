package at.fhv.sysarch.lab2.homeautomation.uihandler;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.http.javadsl.marshalling.sse.EventStreamMarshalling;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.model.sse.ServerSentEvent;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.BroadcastHub;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.stream.OverflowStrategy;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

public class DemoHttpServer extends AllDirectives {

    private final ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment;
    private final ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment;
    private final ActorRef<AirCondition.AirConditionCommand> airCondition;
    private final ActorRef<MediaStation.MediaStationCommand> mediaStation;
    private final ActorRef<Blinds.BlindsCommand> blinds;
    private final String mode;
    private final Template indexTemplate;

    // SSE related
    private final Source<ServerSentEvent, NotUsed> eventSource;

    public DemoHttpServer(ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment,
                          ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment,
                          ActorRef<AirCondition.AirConditionCommand> airCondition,
                          ActorRef<MediaStation.MediaStationCommand> mediaStation,
                          ActorRef<Blinds.BlindsCommand> blinds,
                          String mode,
                          ActorContext<?> context) {
        this.temperatureEnvironment = temperatureEnvironment;
        this.weatherEnvironment = weatherEnvironment;
        this.airCondition = airCondition;
        this.mediaStation = mediaStation;
        this.blinds = blinds;
        this.mode = mode;

        // Setup SSE with BroadcastHub for multiple clients
        Materializer mat = Materializer.matFromSystem(Adapter.toClassic(context.getSystem()));
        
        Source<ServerSentEvent, SourceQueueWithComplete<ServerSentEvent>> queueSource = 
            Source.queue(100, OverflowStrategy.dropTail());
        
        Pair<SourceQueueWithComplete<ServerSentEvent>, Source<ServerSentEvent, NotUsed>> pair = 
            queueSource.toMat(BroadcastHub.of(ServerSentEvent.class), Keep.both()).run(mat);
            
        SourceQueueWithComplete<ServerSentEvent> queue = pair.first();
        this.eventSource = pair.second();

        // Spawn broadcaster actor as child of the controller
        context.spawn(Behaviors.setup(broadcasterContext -> {
            airCondition.tell(new AirCondition.Subscribe(broadcasterContext.messageAdapter(AirCondition.AirConditionStateChanged.class, msg -> msg)));
            blinds.tell(new Blinds.Subscribe(broadcasterContext.messageAdapter(Blinds.BlindsStateChanged.class, msg -> msg)));
            mediaStation.tell(new MediaStation.Subscribe(broadcasterContext.messageAdapter(MediaStation.MediaStationStateChanged.class, msg -> msg)));

            return Behaviors.receive(Object.class)
                    .onMessage(AirCondition.AirConditionStateChanged.class, msg -> {
                        queue.offer(ServerSentEvent.create(msg.poweredOn() ? "ON" : "OFF", "aircondition"));
                        return Behaviors.same();
                    })
                    .onMessage(Blinds.BlindsStateChanged.class, msg -> {
                        queue.offer(ServerSentEvent.create(msg.closed() ? "CLOSED" : "OPEN", "blinds"));
                        return Behaviors.same();
                    })
                    .onMessage(MediaStation.MediaStationStateChanged.class, msg -> {
                        queue.offer(ServerSentEvent.create(msg.playing() ? "PLAYING" : "IDLE", "mediastation"));
                        return Behaviors.same();
                    })
                    .build();
        }), "sseBroadcaster");

        TemplateLoader loader = new ClassPathTemplateLoader("/templates", ".hbs");
        Handlebars handlebars = new Handlebars(loader);
        try {
            this.indexTemplate = handlebars.compile("index");
        } catch (IOException e) {
            throw new RuntimeException("Could not compile handlebars template", e);
        }
    }

    public Route createRoute(ActorSystem<Void> system) {
        return concat(
                pathSingleSlash(() ->
                        get(() -> {
                            CompletionStage<AirCondition.AirConditionStateChanged> acStage = AskPattern.ask(
                                    airCondition,
                                    AirCondition.ReadState::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );
                            CompletionStage<Blinds.BlindsStateChanged> blindsStage = AskPattern.ask(
                                    blinds,
                                    Blinds.ReadState::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );
                            CompletionStage<MediaStation.MediaStationStateChanged> mediaStage = AskPattern.ask(
                                    mediaStation,
                                    MediaStation.ReadState::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );

                            return onSuccess(acStage.thenCombine(blindsStage, Pair::new)
                                            .thenCombine(mediaStage, (pair, med) -> {
                                                Map<String, Object> model = new HashMap<>();
                                                model.put("title", "Home Automation Environment Control");
                                                model.put("mode", this.mode);
                                                model.put("isManual", this.mode.equals("MANUAL"));
                                                model.put("acState", pair.first().poweredOn() ? "ON" : "OFF");
                                                model.put("acClass", pair.first().poweredOn() ? "status-on" : "status-off");
                                                model.put("blindsState", pair.second().closed() ? "CLOSED" : "OPEN");
                                                model.put("blindsClass", pair.second().closed() ? "status-closed" : "status-open");
                                                model.put("mediaState", med.playing() ? "PLAYING" : "IDLE");
                                                model.put("mediaClass", med.playing() ? "status-playing" : "status-idle");
                                                return model;
                                            }),
                                    model -> {
                                        try {
                                            String rendered = indexTemplate.apply(model);
                                            return complete(HttpEntities.create(ContentTypes.TEXT_HTML_UTF8, rendered));
                                        } catch (IOException e) {
                                            return complete("Error rendering template: " + e.getMessage());
                                        }
                                    }
                            );
                        })
                ),
                path("events", () ->
                        get(() -> completeOK(eventSource, EventStreamMarshalling.toEventStream()))
                ),
                path("hello", () -> get(() -> complete("<h1>Say hello to pekko-http</h1>"))),
                path("temperature", () ->
                        post(() -> {
                            if (!this.mode.equals("MANUAL")) {
                                return complete("Manual override is disabled in " + this.mode + " mode");
                            }
                            return formField("value", value -> {
                                double temp = Double.parseDouble(value);
                                this.temperatureEnvironment.tell(new TemperatureEnvironment.SetTemperature(temp));
                                return complete("Temperature set to " + temp);
                            });
                        })
                ),
                path("weather", () ->
                        post(() -> {
                            if (!this.mode.equals("MANUAL")) {
                                return complete("Manual override is disabled in " + this.mode + " mode");
                            }
                            return formField("value", value -> {
                                WeatherEnvironment.Weather weather = WeatherEnvironment.Weather.valueOf(value.toUpperCase());
                                this.weatherEnvironment.tell(new WeatherEnvironment.SetWeather(weather));
                                return complete("Weather set to " + weather);
                            });
                        })
                ),
                path("aircondition", () ->
                        post(() -> formField("value", value -> {
                            boolean power = Boolean.parseBoolean(value);
                            this.airCondition.tell(new AirCondition.PowerAirCondition(power));
                            return complete("AirCondition power set to " + power);
                        }))
                ),
                path("mediastation-play", () ->
                        post(() -> {
                            System.out.println("Server: Received MediaStation Play request");
                            this.mediaStation.tell(new MediaStation.PlayMovie());
                            return complete("MediaStation: Play movie");
                        })
                ),
                path("mediastation-stop", () ->
                        post(() -> {
                            System.out.println("Server: Received MediaStation Stop request");
                            this.mediaStation.tell(new MediaStation.StopMovie());
                            return complete("MediaStation: Stop movie");
                        })
                ),
                path("blinds", () ->
                        post(() -> formField("value", value -> {
                            boolean close = Boolean.parseBoolean(value);
                            this.blinds.tell(new Blinds.ControlBlinds(close));
                            return complete("Blinds " + (close ? "closed" : "opened"));
                        }))
                )
        );
    }
}
