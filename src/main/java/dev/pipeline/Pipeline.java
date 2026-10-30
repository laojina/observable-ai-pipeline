package dev.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class Pipeline {
    private static final List<String> NAMES = List.of("VALIDATE", "EXTRACT_FRAMES", "MOCK_REPLACE", "PACKAGE");
    private final JdbcTemplate database;
    private final TransactionTemplate transaction;
    private final ObjectMapper json;
    private final Path artifacts;
    private final boolean enabled;
    private final AtomicBoolean busy = new AtomicBoolean();
    private final ExecutorService worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());

    public Pipeline(JdbcTemplate database, TransactionTemplate transaction, ObjectMapper json,
                    @Value("${pipeline.data}") String directory,
                    @Value("${pipeline.enabled:true}") boolean enabled) {
        this.database = database;
        this.transaction = transaction;
        this.json = json;
        this.artifacts = Path.of(directory).toAbsolutePath().normalize().resolve("artifacts");
        this.enabled = enabled;
    }

    @PostConstruct
    public synchronized void recover() throws Exception {
        Files.createDirectories(artifacts);
        transaction.executeWithoutResult(status -> {
            database.update("UPDATE attempts SET status='ABANDONED', ended_at=?, error='Process interrupted' WHERE status='RUNNING'", now());
            database.update("UPDATE steps SET status='PENDING', error='Recovered after process restart' WHERE status='RUNNING'");
            for (Map<String, Object> task : database.queryForList("SELECT * FROM tasks WHERE status='RUNNING'")) {
                event(text(task, "ID"), "RECOVERED: previous process stopped; incomplete step will be retried");
            }
            database.update("UPDATE tasks SET status='QUEUED' WHERE status='RUNNING'");
        });
    }

    public synchronized String submit(TaskController.Submission input) {
        String asset = input.asset() == null ? "demo-animation" : input.asset().trim();
        String revision = input.revision() == null ? "v1" : input.revision().trim();
        long duration = input.durationMs() == null ? 1500 : input.durationMs();
        int failStep = input.failStep() == null ? 0 : input.failStep();
        int failTimes = input.failTimes() == null ? 0 : input.failTimes();
        long timeout = input.timeoutMs() == null ? 30000 : input.timeoutMs();
        if (asset.isEmpty() || asset.length() > 200 || revision.isEmpty() || revision.length() > 80
                || duration < 0 || duration > 3600000 || timeout < 100 || timeout > 7200000
                || failStep < 0 || failStep > 4 || failTimes < 0 || failTimes > 100) {
            throw new IllegalArgumentException("Invalid asset/revision/duration/failure/timeout configuration");
        }
        String id = UUID.randomUUID().toString();
        transaction.executeWithoutResult(status -> {
            database.update("INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?)", id, asset, revision, "QUEUED", duration, failStep, failTimes, timeout, now());
            for (int position = 1; position <= 4; position++) {
                database.update("INSERT INTO steps(task_id,position,name,status) VALUES(?,?,?,'PENDING')", id, position, NAMES.get(position - 1));
            }
            event(id, "SUBMITTED: all computation stages are explicit MOCK adapters");
        });
        return id;
    }

    public synchronized List<Map<String, Object>> list() {
        return database.queryForList("SELECT * FROM tasks ORDER BY created_at DESC");
    }

    public synchronized Map<String, Object> detail(String id) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("task", task(id));
        result.put("steps", steps(id));
        result.put("attempts", database.queryForList("SELECT * FROM attempts WHERE task_id=? ORDER BY started_at,id", id));
        result.put("events", database.queryForList("SELECT * FROM events WHERE task_id=? ORDER BY id", id));
        return result;
    }

    public synchronized Object artifact(String id, int position) {
        task(id);
        List<Map<String, Object>> found = database.queryForList("SELECT * FROM steps WHERE task_id=? AND position=? AND status='SUCCEEDED'", id, position);
        if (found.isEmpty() || !valid(found.get(0))) throw new IllegalArgumentException("Artifact is missing or invalid; resume the task to rebuild it");
        try { return json.readTree(Files.readAllBytes(Path.of(text(found.get(0), "ARTIFACT")))); }
        catch (Exception error) { throw new IllegalArgumentException("Artifact cannot be read", error); }
    }

    public synchronized void retry(String id) {
        Map<String, Object> task = task(id);
        if (!List.of("FAILED", "SUCCEEDED").contains(text(task, "STATUS"))) {
            throw new IllegalArgumentException("Only terminal tasks can be resumed");
        }
        transaction.executeWithoutResult(status -> {
            database.update("UPDATE steps SET max_attempts=attempts+3 WHERE task_id=?", id);
            database.update("UPDATE steps SET status='PENDING', next_run_at=0 WHERE task_id=? AND status='FAILED'", id);
            database.update("UPDATE tasks SET status='QUEUED' WHERE id=?", id);
            event(id, "MANUAL_RESUME: verify completed artifacts before reusing them");
        });
    }

    @Scheduled(fixedDelay = 250)
    public void dispatch() {
        if (!enabled || !busy.compareAndSet(false, true)) return;
        try {
            worker.submit(() -> {
                try { tick(); }
                catch (Exception error) {
                    System.getLogger(Pipeline.class.getName()).log(System.Logger.Level.ERROR, "Worker iteration failed", error);
                } finally { busy.set(false); }
            });
        } catch (RejectedExecutionException error) { busy.set(false); }
    }

    @Scheduled(fixedDelay = 1000)
    public void heartbeat() {
        if (enabled) database.update("UPDATE steps SET heartbeat=? WHERE status='RUNNING'", now());
    }

    public void tick() throws Exception {
        Map<String, Object> selected;
        Map<String, Object> selectedStep = null;
        String attemptId = UUID.randomUUID().toString();
        synchronized (this) {
            List<Map<String, Object>> ready = database.queryForList("SELECT * FROM tasks WHERE status IN ('QUEUED','RUNNING') ORDER BY created_at");
            if (ready.isEmpty()) return;
            selected = ready.get(0);
            String id = text(selected, "ID");
            List<Map<String, Object>> current = steps(id);
            String upstream = "root";
            for (Map<String, Object> step : current) {
                int position = number(step, "POSITION").intValue();
                String fingerprint = hash(json.writeValueAsBytes(List.of(text(selected, "ASSET"), text(selected, "REVISION"), position, "mock-adapter-v1", upstream)));
                if (text(step, "STATUS").equals("SUCCEEDED") && fingerprint.equals(text(step, "FINGERPRINT")) && valid(step)) {
                    upstream = text(step, "ARTIFACT_HASH");
                    continue;
                }
                if (text(step, "STATUS").equals("RUNNING")) return;
                if (text(step, "STATUS").equals("RETRY_WAIT") && number(step, "NEXT_RUN_AT").longValue() > now()) return;
                selectedStep = step;
                step.put("FINGERPRINT", fingerprint);
                break;
            }
            if (selectedStep == null) {
                transaction.executeWithoutResult(status -> {
                    database.update("UPDATE tasks SET status='SUCCEEDED' WHERE id=?", id);
                    event(id, "COMPLETED: all four stage artifacts verified");
                });
                return;
            }
            int position = number(selectedStep, "POSITION").intValue();
            int attempt = number(selectedStep, "ATTEMPTS").intValue() + 1;
            if (attempt > number(selectedStep, "MAX_ATTEMPTS").intValue()) {
                transaction.executeWithoutResult(status -> {
                    database.update("UPDATE steps SET status='FAILED',error='Attempt budget exhausted after interruption' WHERE task_id=? AND position=?", id, position);
                    database.update("UPDATE tasks SET status='FAILED' WHERE id=?", id);
                    event(id, "FAILED: interrupted executions exhausted attempt budget");
                });
                return;
            }
            transaction.executeWithoutResult(status -> {
                database.update("UPDATE tasks SET status='RUNNING' WHERE id=?", id);
                database.update("UPDATE steps SET status='PENDING',artifact=NULL,artifact_hash=NULL,fingerprint=NULL WHERE task_id=? AND position>?", id, position);
                database.update("UPDATE steps SET status='RUNNING', attempts=?, heartbeat=?, error=NULL WHERE task_id=? AND position=?", attempt, now(), id, position);
                database.update("INSERT INTO attempts(id,task_id,position,attempt_no,status,started_at) VALUES(?,?,?,?,'RUNNING',?)", attemptId, id, position, attempt, now());
                event(id, "START step=" + position + " attempt=" + attempt);
            });
            selectedStep.put("ATTEMPTS", attempt);
        }
        execute(selected, selectedStep, attemptId);
    }

    private void execute(Map<String, Object> task, Map<String, Object> step, String attemptId) throws Exception {
        String id = text(task, "ID");
        int position = number(step, "POSITION").intValue();
        try {
            String fingerprint = text(step, "FINGERPRINT");
            byte[] content = json.writeValueAsBytes(new TreeMap<>(Map.of("fingerprint", fingerprint, "step", NAMES.get(position - 1), "asset", text(task, "ASSET"), "mock", true)));
            Path output = artifacts.resolve(fingerprint + ".json");
            boolean reused = Files.isRegularFile(output) && Arrays.equals(content, Files.readAllBytes(output));
            if (!reused) {
                long start = System.nanoTime();
                long duration = number(task, "DURATION_MS").longValue();
                while (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < duration) {
                    if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > number(task, "TIMEOUT_MS").longValue()) throw new TimeoutException("Mock step execution timed out");
                    Thread.sleep(Math.min(100, Math.max(1, duration)));
                }
                if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > number(task, "TIMEOUT_MS").longValue()) throw new TimeoutException("Mock step execution timed out");
                if (position == number(task, "FAIL_STEP").intValue() && number(step, "ATTEMPTS").intValue() <= number(task, "FAIL_TIMES").intValue()) throw new IllegalStateException("Injected deterministic failure");
                Path temporary = Files.createTempFile(artifacts, "writing-", ".tmp");
                Files.write(temporary, content);
                Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            synchronized (this) {
                transaction.executeWithoutResult(status -> {
                    database.update("UPDATE steps SET status='SUCCEEDED', artifact=?, artifact_hash=?, fingerprint=?, error=NULL WHERE task_id=? AND position=?", output.toString(), hash(content), fingerprint, id, position);
                    database.update("UPDATE attempts SET status=?, ended_at=? WHERE id=?", reused ? "REUSED" : "SUCCEEDED", now(), attemptId);
                    event(id, (reused ? "REUSED" : "SUCCEEDED") + " step=" + position);
                });
            }
        } catch (Exception error) {
            synchronized (this) {
                boolean exhausted = number(step, "ATTEMPTS").intValue() >= number(step, "MAX_ATTEMPTS").intValue();
                String message = error.getClass().getSimpleName() + ": " + error.getMessage();
                String safe = message.substring(0, Math.min(message.length(), 900));
                transaction.executeWithoutResult(status -> {
                    database.update("UPDATE steps SET status=?, error=?, next_run_at=? WHERE task_id=? AND position=?", exhausted ? "FAILED" : "RETRY_WAIT", safe, now() + 500L * number(step, "ATTEMPTS").intValue(), id, position);
                    database.update("UPDATE attempts SET status='FAILED', ended_at=?, error=? WHERE id=?", now(), safe, attemptId);
                    database.update("UPDATE tasks SET status=? WHERE id=?", exhausted ? "FAILED" : "QUEUED", id);
                    event(id, "FAILED step=" + position + ": " + safe);
                });
            }
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private boolean valid(Map<String, Object> step) {
        try { return Files.isRegularFile(Path.of(text(step, "ARTIFACT"))) && hash(Files.readAllBytes(Path.of(text(step, "ARTIFACT")))).equals(text(step, "ARTIFACT_HASH")); }
        catch (Exception error) { return false; }
    }

    private Map<String, Object> task(String id) {
        List<Map<String, Object>> found = database.queryForList("SELECT * FROM tasks WHERE id=?", id);
        if (found.isEmpty()) throw new IllegalArgumentException("Task not found");
        return found.get(0);
    }

    private List<Map<String, Object>> steps(String id) { return database.queryForList("SELECT * FROM steps WHERE task_id=? ORDER BY position", id); }
    private void event(String id, String message) { database.update("INSERT INTO events(task_id,recorded_at,message) VALUES(?,?,?)", id, now(), message); }
    private static String text(Map<String, Object> row, String key) { return Objects.toString(row.get(key), ""); }
    private static Number number(Map<String, Object> row, String key) { return (Number) row.get(key); }
    private static long now() { return System.currentTimeMillis(); }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception error) { throw new IllegalStateException(error); }
    }
    @PreDestroy
    public void close() { worker.shutdownNow(); }
}
