package pdi.lab.producer_signals.controller;

import java.time.Instant;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pdi.lab.contracts.PositionSignal;
import pdi.lab.producer_signals.model.SignalRequest;

@RestController
@RequestMapping("/signals")
public class PositionSignalsController {

    private static final String TOPIC = "signals";
    private final KafkaTemplate<String, PositionSignal> kafka;

    public PositionSignalsController(KafkaTemplate<String, PositionSignal> kafka) {
        this.kafka = kafka;
    }

    @PostMapping
    public ResponseEntity<Void> publish(@RequestBody SignalRequest req) {
        PositionSignal signal = PositionSignal.newBuilder()
                .setIdentifier(req.identifier())
                .setLatitude(req.latitude())
                .setLongitude(req.longitude())
                .setTimestamp(Instant.now())
                .setVelocity(req.velocity())
                .build();

        // join() deixa o envio síncrono de propósito: erros de contrato aparecem na resposta HTTP
        kafka.send(TOPIC, signal.getIdentifier(), signal).join();
        return ResponseEntity.accepted().build();
    }
}