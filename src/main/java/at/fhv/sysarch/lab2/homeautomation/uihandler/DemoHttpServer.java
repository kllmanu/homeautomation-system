package at.fhv.sysarch.lab2.homeautomation.uihandler;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.fridge.FridgeModels;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.http.javadsl.marshalling.sse.EventStreamMarshalling;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.model.sse.ServerSentEvent;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.BroadcastHub;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class DemoHttpServer extends AllDirectives {

    private final ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment;
    private final ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment;
    private final ActorRef<AirCondition.AirConditionCommand> airCondition;
    private final ActorRef<MediaStation.MediaStationCommand> mediaStation;
    private final ActorRef<Blinds.BlindsCommand> blinds;
    private final ActorRef<FridgeModels.FridgeCommand> fridge;
    private final String mode;
    private final Template indexTemplate;

    // SSE related
    private final Source<ServerSentEvent, NotUsed> eventSource;

    // Broadcaster protocol
    public interface BroadcasterCommand {}
    public record AirConditionUpdate(AirCondition.AirConditionStateChanged msg) implements BroadcasterCommand {}
    public record BlindsUpdate(Blinds.BlindsStateChanged msg) implements BroadcasterCommand {}
    public record MediaStationUpdate(MediaStation.MediaStationStateChanged msg) implements BroadcasterCommand {}
    public record TemperatureUpdate(TemperatureEnvironment.TemperatureResponse msg) implements BroadcasterCommand {}
    public record WeatherUpdate(WeatherEnvironment.WeatherResponse msg) implements BroadcasterCommand {}
    public record FridgeUpdate(FridgeModels.FridgeUpdate msg) implements BroadcasterCommand {}
    private enum Heartbeat implements BroadcasterCommand { INSTANCE }

    public DemoHttpServer(ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment,
                          ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment,
                          ActorRef<AirCondition.AirConditionCommand> airCondition,
                          ActorRef<MediaStation.MediaStationCommand> mediaStation,
                          ActorRef<Blinds.BlindsCommand> blinds,
                          ActorRef<FridgeModels.FridgeCommand> fridge,
                          String mode,
                          ActorContext<?> context) {
        this.temperatureEnvironment = temperatureEnvironment;
        this.weatherEnvironment = weatherEnvironment;
        this.airCondition = airCondition;
        this.mediaStation = mediaStation;
        this.blinds = blinds;
        this.fridge = fridge;
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
        Behavior<BroadcasterCommand> broadcasterBehavior = Behaviors.setup(broadcasterContext -> Behaviors.withTimers(timers -> {
            timers.startTimerAtFixedRate(Heartbeat.INSTANCE, Duration.ofSeconds(10));

            airCondition.tell(new AirCondition.Subscribe(broadcasterContext.messageAdapter(AirCondition.AirConditionStateChanged.class, AirConditionUpdate::new)));
            blinds.tell(new Blinds.Subscribe(broadcasterContext.messageAdapter(Blinds.BlindsStateChanged.class, BlindsUpdate::new)));
            mediaStation.tell(new MediaStation.Subscribe(broadcasterContext.messageAdapter(MediaStation.MediaStationStateChanged.class, MediaStationUpdate::new)));
            temperatureEnvironment.tell(new TemperatureEnvironment.Subscribe(broadcasterContext.messageAdapter(TemperatureEnvironment.TemperatureResponse.class, TemperatureUpdate::new)));
            weatherEnvironment.tell(new WeatherEnvironment.Subscribe(broadcasterContext.messageAdapter(WeatherEnvironment.WeatherResponse.class, WeatherUpdate::new)));
            fridge.tell(new FridgeModels.Subscribe(broadcasterContext.messageAdapter(FridgeModels.FridgeUpdate.class, FridgeUpdate::new)));

            return Behaviors.receive(BroadcasterCommand.class)
                    .onMessage(AirConditionUpdate.class, u -> {
                        AirCondition.AirConditionStateChanged msg = u.msg();
                        broadcasterContext.getLog().info("Broadcasting AC state: {}", msg.poweredOn());
                        queue.offer(ServerSentEvent.create(msg.poweredOn() ? "ON" : "OFF", "aircondition"));
                        return Behaviors.same();
                    })
                    .onMessage(BlindsUpdate.class, u -> {
                        Blinds.BlindsStateChanged msg = u.msg();
                        broadcasterContext.getLog().info("Broadcasting Blinds state: {}", msg.closed());
                        queue.offer(ServerSentEvent.create(msg.closed() ? "CLOSED" : "OPEN", "blinds"));
                        return Behaviors.same();
                    })
                    .onMessage(MediaStationUpdate.class, u -> {
                        MediaStation.MediaStationStateChanged msg = u.msg();
                        broadcasterContext.getLog().info("Broadcasting MediaStation state: {}", msg.playing());
                        queue.offer(ServerSentEvent.create(msg.playing() ? "PLAYING" : "IDLE", "mediastation"));
                        return Behaviors.same();
                    })
                    .onMessage(TemperatureUpdate.class, u -> {
                        TemperatureEnvironment.TemperatureResponse msg = u.msg();
                        broadcasterContext.getLog().info("Broadcasting Temperature: {}", msg.value());
                        queue.offer(ServerSentEvent.create(String.format("%.1f", msg.value()), "temperature"));
                        return Behaviors.same();
                    })
                    .onMessage(WeatherUpdate.class, u -> {
                        WeatherEnvironment.WeatherResponse msg = u.msg();
                        broadcasterContext.getLog().info("Broadcasting Weather: {}", msg.weather());
                        queue.offer(ServerSentEvent.create(msg.weather().toString(), "weather"));
                        return Behaviors.same();
                    })
                    .onMessage(FridgeUpdate.class, u -> {
                        broadcasterContext.getLog().info("Broadcasting Fridge update");
                        queue.offer(ServerSentEvent.create("update", "fridge-update"));
                        return Behaviors.same();
                    })
                    .onMessage(Heartbeat.class, h -> {
                        queue.offer(ServerSentEvent.create("ping", "heartbeat"));
                        return Behaviors.same();
                    })
                    .build();
        }));
        
        ((ActorContext)context).spawn(broadcasterBehavior, "sseBroadcaster");

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
                            CompletionStage<TemperatureEnvironment.TemperatureResponse> tempStage = AskPattern.ask(
                                    temperatureEnvironment,
                                    TemperatureEnvironment.GetTemperature::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );
                            CompletionStage<WeatherEnvironment.WeatherResponse> weatherStage = AskPattern.ask(
                                    weatherEnvironment,
                                    WeatherEnvironment.GetWeather::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );
                            CompletionStage<FridgeModels.ProductsResponse> fridgeProductsStage = AskPattern.ask(
                                    fridge,
                                    FridgeModels.QueryProducts::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );
                            CompletionStage<FridgeModels.OrderHistoryResponse> fridgeHistoryStage = AskPattern.ask(
                                    fridge,
                                    FridgeModels.QueryOrderHistory::new,
                                    Duration.ofSeconds(2),
                                    system.scheduler()
                            );

                            return onSuccess(acStage.thenCombine(blindsStage, Pair::new)
                                            .thenCombine(mediaStage, Pair::new)
                                            .thenCombine(tempStage, Pair::new)
                                            .thenCombine(weatherStage, Pair::new)
                                            .thenCombine(fridgeProductsStage, Pair::new)
                                            .thenCombine(fridgeHistoryStage, Pair::new),
                                    combined -> {
                                        // Nesting: Pair<Pair<Pair<Pair<Pair<Pair<AC, Blinds>, Media>, Temp>, Weather>, Products>, History>
                                        var p_products = combined.first(); // Pair<Pair<Pair<Pair<Pair<AC, Blinds>, Media>, Temp>, Weather>, Products>
                                        var p_weather = p_products.first(); // Pair<Pair<Pair<Pair<AC, Blinds>, Media>, Temp>, Weather>
                                        var p_temp = p_weather.first(); // Pair<Pair<Pair<AC, Blinds>, Media>, Temp>
                                        var p_media = p_temp.first(); // Pair<Pair<AC, Blinds>, Media>
                                        var p_ac_blinds = p_media.first(); // Pair<AC, Blinds>

                                        AirCondition.AirConditionStateChanged ac = p_ac_blinds.first();
                                        Blinds.BlindsStateChanged blindsState = p_ac_blinds.second();
                                        MediaStation.MediaStationStateChanged media = p_media.second();
                                        TemperatureEnvironment.TemperatureResponse temp = p_temp.second();
                                        WeatherEnvironment.WeatherResponse weather = p_weather.second();
                                        FridgeModels.ProductsResponse products = p_products.second();
                                        FridgeModels.OrderHistoryResponse history = combined.second();

                                        Map<String, Object> model = new HashMap<>();
                                        model.put("title", "Home Automation Environment Control");
                                        model.put("mode", this.mode);
                                        model.put("isManual", this.mode.equals("MANUAL"));

                                        model.put("acState", ac.poweredOn() ? "ON" : "OFF");
                                        model.put("acClass", ac.poweredOn() ? "status-on" : "status-off");
                                        model.put("blindsState", blindsState.closed() ? "CLOSED" : "OPEN");
                                        model.put("blindsClass", blindsState.closed() ? "status-closed" : "status-open");
                                        model.put("mediaState", media.playing() ? "PLAYING" : "IDLE");
                                        model.put("mediaClass", media.playing() ? "status-playing" : "status-idle");
                                        
                                        model.put("temperature", String.format("%.1f", temp.value()));
                                        model.put("tempEmoji", temp.value() < 10 ? "❄️" : (temp.value() < 20 ? "🌡️" : (temp.value() < 30 ? "☀️" : "🔥")));
                                        
                                        String wString = weather.weather().toString();
                                        model.put("weather", wString);
                                        model.put("weatherEmoji", wString.contains("SUNNY") ? "☀️" : (wString.contains("CLOUDY") ? "☁️" : (wString.contains("RAINY") ? "🌧️" : "🛸")));
                                        
                                        model.put("fridgeProducts", products.products());
                                        model.put("fridgeHistory", history.orders());
                                        
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
                pathPrefix("fridge", () -> concat(
                        path("consume", () -> post(() -> formField("productName", name -> {
                            this.fridge.tell(new FridgeModels.ConsumeProduct(name));
                            return complete("Consumed " + name);
                        }))),
                        path("order", () -> post(() -> formField("productName", name -> formField("quantity", q -> {
                            // Dummy price/weight for demo
                            FridgeModels.Product p = new FridgeModels.Product(name, 2.5, 0.5);
                            this.fridge.tell(new FridgeModels.OrderProduct(p, Integer.parseInt(q)));
                            return complete("Ordering " + q + "x " + name);
                        }))))
                )),
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
