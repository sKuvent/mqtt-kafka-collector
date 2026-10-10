package dev.mqttkafka;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Admission control for in-flight records; not a JVM heap hard limit. */
public final class ByteBudget {
    private final long maxBytes;
    private final int maxMessages;
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicInteger records = new AtomicInteger();

    public ByteBudget(long maxBytes, int maxMessages) {
        if (maxBytes <= 0 || maxMessages <= 0) throw new IllegalArgumentException("Invalid buffer limits");
        this.maxBytes = maxBytes;
        this.maxMessages = maxMessages;
    }
    public synchronized boolean reserve(long charge) {
        if (charge <= 0 || charge > maxBytes || records.get() >= maxMessages || bytes.get() > maxBytes - charge) return false;
        bytes.addAndGet(charge);
        records.incrementAndGet();
        return true;
    }
    public synchronized void release(long charge) {
        bytes.addAndGet(-charge);
        records.decrementAndGet();
    }
    public long bytes() { return bytes.get(); }
    public int records() { return records.get(); }
    public long maxBytes() { return maxBytes; }
    public int maxMessages() { return maxMessages; }
}
