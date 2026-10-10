package dev.mqttkafka;

@FunctionalInterface
interface CollectorReadiness {
    boolean isReady();
}
