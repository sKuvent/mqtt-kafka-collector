package dev.mqttkafka;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ByteBudgetTest {
    @Test void boundsAndReleasesBytesAndMessages() {
        var budget = new ByteBudget(100, 2);
        assertTrue(budget.reserve(60));
        assertFalse(budget.reserve(41));
        assertTrue(budget.reserve(40));
        assertFalse(budget.reserve(1));
        assertEquals(100, budget.bytes());
        budget.release(60);
        assertEquals(1, budget.records());
        assertTrue(budget.reserve(50));
    }
}
