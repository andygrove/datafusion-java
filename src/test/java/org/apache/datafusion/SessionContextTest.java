package org.apache.datafusion;

import org.junit.jupiter.api.Test;

class SessionContextTest {
    @Test
    void canExecuteSelect1() {
        try (SessionContext ctx = new SessionContext()) {
            ctx.sql("SELECT 1");
        }
    }
}
