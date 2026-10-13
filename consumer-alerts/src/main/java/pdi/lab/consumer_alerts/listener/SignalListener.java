package pdi.lab.consumer_alerts.listener;

import pdi.lab.contracts.PositionSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SignalListener {
    private static final Logger log = LoggerFactory.getLogger(SignalListener.class);

    @KafkaListener(topics = "signals")
    public void consumer(PositionSignal sinal) {
        log.info("Recebido: {}", sinal);
        Double velocidade = sinal.getVelocity();
        if (velocidade != null && velocidade > 120) {
            log.warn("ALERTA: {} a {} km/h", sinal.getIdentifier(), velocidade);
        }
    }
}
