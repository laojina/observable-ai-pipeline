package dev.pipeline;

import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {"pipeline.enabled=false", "pipeline.data=./target/test-data", "spring.datasource.url=jdbc:h2:mem:pipeline-tests"})
class PipelineTest {
    @Autowired Pipeline pipeline;
    @Autowired JdbcTemplate database;

    @BeforeEach
    void clear() {
        for (String table : List.of("events", "attempts", "steps", "tasks")) database.update("DELETE FROM " + table);
    }

    private String submit(String asset, int failTimes) {
        return pipeline.submit(new TaskController.Submission(asset, "v1", 0L, 3, failTimes, 1000L));
    }

    private String state(String id) { return database.queryForObject("SELECT status FROM tasks WHERE id=?", String.class, id); }

    private void settle(String id) throws Exception {
        for (int iteration = 0; iteration < 20 && !List.of("FAILED", "SUCCEEDED").contains(state(id)); iteration++) {
            database.update("UPDATE steps SET next_run_at=0 WHERE task_id=?", id);
            pipeline.tick();
        }
    }

    @Test void successfulPipelineAndCrossTaskReuse() throws Exception {
        String asset = UUID.randomUUID().toString();
        String original = submit(asset, 0);
        settle(original);
        assertEquals("SUCCEEDED", state(original));
        String duplicate = submit(asset, 0);
        settle(duplicate);
        assertEquals(4, database.queryForObject("SELECT COUNT(*) FROM attempts WHERE task_id=? AND status='REUSED'", Integer.class, duplicate));
    }

    @Test void transientFailureRetriesOnlyFailedStage() throws Exception {
        String id = submit(UUID.randomUUID().toString(), 1);
        settle(id);
        assertEquals("SUCCEEDED", state(id));
        assertEquals(List.of(1, 1, 2, 1), database.queryForList("SELECT attempts FROM steps WHERE task_id=? ORDER BY position", Integer.class, id));
    }

    @Test void exhaustedFailuresStopAndManualRetryIsGuarded() throws Exception {
        String id = submit(UUID.randomUUID().toString(), 3);
        settle(id);
        assertEquals("FAILED", state(id));
        pipeline.retry(id);
        assertThrows(IllegalArgumentException.class, () -> pipeline.retry(id));
        settle(id);
        assertEquals("SUCCEEDED", state(id));
    }

    @Test void startupRecoversAnInterruptedAttempt() throws Exception {
        String id = submit(UUID.randomUUID().toString(), 0);
        pipeline.tick();
        pipeline.tick();
        database.update("UPDATE tasks SET status='RUNNING' WHERE id=?", id);
        database.update("UPDATE steps SET status='RUNNING', attempts=1 WHERE task_id=? AND position=3", id);
        database.update("INSERT INTO attempts(id,task_id,position,attempt_no,status,started_at) VALUES(?,?,3,1,'RUNNING',?)", UUID.randomUUID().toString(), id, System.currentTimeMillis());
        pipeline.recover();
        settle(id);
        assertEquals("SUCCEEDED", state(id));
        assertEquals(List.of(1, 1, 2, 1), database.queryForList("SELECT attempts FROM steps WHERE task_id=? ORDER BY position", Integer.class, id));
        assertEquals(1, database.queryForObject("SELECT COUNT(*) FROM attempts WHERE task_id=? AND status='ABANDONED'", Integer.class, id));
    }

    @Test void changedConfigurationDoesNotReuseOldResults() throws Exception {
        String asset = UUID.randomUUID().toString();
        settle(submit(asset, 0));
        String changed = pipeline.submit(new TaskController.Submission(asset, "v2", 0L, 0, 0, 1000L));
        settle(changed);
        assertEquals(0, database.queryForObject("SELECT COUNT(*) FROM attempts WHERE task_id=? AND status='REUSED'", Integer.class, changed));
    }

    @Test void timeoutStopsBeforeDownstream() throws Exception {
        String id = pipeline.submit(new TaskController.Submission(UUID.randomUUID().toString(), "v1", 150L, 0, 0, 100L));
        settle(id);
        assertEquals("FAILED", state(id));
        assertEquals(3, database.queryForObject("SELECT attempts FROM steps WHERE task_id=? AND position=1", Integer.class, id));
        assertEquals(0, database.queryForObject("SELECT SUM(attempts) FROM steps WHERE task_id=? AND position>1", Integer.class, id));
    }

    @Test void manualResumeCanRegenerateArtifactAfterOriginalBudgetUsed() throws Exception {
        String id = submit(UUID.randomUUID().toString(), 2);
        settle(id);
        assertEquals("SUCCEEDED", state(id));
        String file = database.queryForObject("SELECT artifact FROM steps WHERE task_id=? AND position=3", String.class, id);
        java.nio.file.Files.writeString(java.nio.file.Path.of(file), "damaged");
        pipeline.retry(id);
        settle(id);
        assertEquals("SUCCEEDED", state(id));
        assertEquals(4, database.queryForObject("SELECT attempts FROM steps WHERE task_id=? AND position=3", Integer.class, id));
    }

    @Test void inputsWithSeparatorsHaveDifferentFingerprints() throws Exception {
        String asset = UUID.randomUUID().toString();
        String first = pipeline.submit(new TaskController.Submission(asset + "\npart", "v1", 0L, 0, 0, 1000L));
        settle(first);
        String second = pipeline.submit(new TaskController.Submission(asset, "part\nv1", 0L, 0, 0, 1000L));
        settle(second);
        assertNotEquals(database.queryForObject("SELECT fingerprint FROM steps WHERE task_id=? AND position=1", String.class, first), database.queryForObject("SELECT fingerprint FROM steps WHERE task_id=? AND position=1", String.class, second));
    }
}
