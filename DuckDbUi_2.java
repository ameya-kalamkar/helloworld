/*
 * DuckDbUi - a zero-install web console for DuckDB.
 *
 * One file, JDK only (Java 11+). Needs nothing but the DuckDB JDBC jar that is already on the server.
 * Nothing to deploy, no build step:
 *
 *     java -cp /path/to/duckdb_jdbc.jar DuckDbUi.java
 *
 * Then open http://<server>:8080 in your browser.
 *
 * Options:
 *   --port N            HTTP port (default 8080)
 *   --host ADDR         bind address (default 0.0.0.0; use 127.0.0.1 with an SSH tunnel)
 *   --db PATH           DuckDB database file (default: in-memory)
 *   --read-only         open --db read-only
 *   --root DIR          restrict the file browser to DIR (default: /)
 *   --auth USER:PASS    require HTTP Basic auth (recommended on shared servers)
 *   --memory 8GB        DuckDB memory limit for the whole instance   (overrides CONFIG below)
 *   --threads N         DuckDB worker threads                        (overrides CONFIG below)
 *   --temp-dir DIR      spill directory for large queries            (overrides CONFIG below)
 *   --max-connections N max concurrent queries                       (overrides CONFIG below)
 *
 * Every request gets its own DuckDB connection (a duplicate of the main one), so queries from different
 * users run in parallel. Temp tables / SET / USE are therefore per-request; CREATE TABLE and ATTACH are
 * shared by everyone. Tune the defaults in the CONFIG block below.
 *
 * Note: this runs arbitrary SQL as the OS user who started it, and DuckDB can read/write any file that
 * user can. Run it as a low-privilege user, use --auth, and stop it when you are done.
 */
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class DuckDbUi {

    // =====================================================================================================
    // CONFIG - global DuckDB settings and limits. Edit here (or override with the command-line options).
    // These are applied once at startup to the DuckDB instance and are shared by every connection.
    // =====================================================================================================
    static int    MAX_CONNECTIONS     = 10;        // max queries running at once; extra requests wait, then get HTTP 503
    static int    QUEUE_WAIT_SECONDS  = 60;        // how long a request waits for a free connection slot
    static String MEMORY_LIMIT        = "4GB";     // memory_limit: total RAM for the whole instance (all connections); "" = DuckDB default
    static int    THREADS             = 4;         // threads: worker threads for the whole instance; 0 = DuckDB default (all cores)
    static String TEMP_DIRECTORY      = System.getProperty("java.io.tmpdir") + "/duckdb_tmp"; // spill-to-disk location; "" = DuckDB default
    static String MAX_TEMP_DIR_SIZE   = "";        // max_temp_directory_size, e.g. "50GB"; "" = DuckDB default
    static boolean PRESERVE_INSERTION_ORDER = false; // false = faster and lower memory for big scans/exports (row order not guaranteed)
    static String[] EXTRA_SETTINGS    = {          // any other SET statements, run at startup, e.g. "SET enable_progress_bar=false"
    };
    // =====================================================================================================

    static Connection base;                                   // keeps the database instance alive; only used to spawn per-request connections
    static Semaphore slots;
    static final Map<String, Statement> RUNNING = new ConcurrentHashMap<>();   // query id -> running statement (for Cancel)
    static Path root = Paths.get("/").toAbsolutePath().normalize();
    static String expectedAuth;
    static String dbLabel = ":memory:";
    static boolean readOnly;
    static String version = "?";
    static byte[] page;

    public static void main(String[] a) throws Exception {
        int port = 8080;
        String host = "0.0.0.0", db = null, auth = null;
        for (int i = 0; i < a.length; i++) {
            switch (a[i]) {
                case "--port": port = Integer.parseInt(a[++i]); break;
                case "--host": host = a[++i]; break;
                case "--db": db = a[++i]; break;
                case "--read-only": readOnly = true; break;
                case "--root": root = Paths.get(a[++i]).toAbsolutePath().normalize(); break;
                case "--auth": auth = a[++i]; break;
                case "--memory": MEMORY_LIMIT = a[++i]; break;
                case "--threads": THREADS = Integer.parseInt(a[++i]); break;
                case "--temp-dir": TEMP_DIRECTORY = a[++i]; break;
                case "--max-connections": MAX_CONNECTIONS = Integer.parseInt(a[++i]); break;
                default:
                    System.err.println("Unknown option: " + a[i] + "\nSee the header of this file for options.");
                    System.exit(2);
            }
        }
        if (auth != null) {
            if (!auth.contains(":")) { System.err.println("--auth must be USER:PASS"); System.exit(2); }
            expectedAuth = "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        }
        StringBuilder html = new StringBuilder();
        for (String chunk : HTML_B64) html.append(chunk);
        page = Base64.getDecoder().decode(html.toString());

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB JDBC driver not found. Start with:  java -cp /path/to/duckdb_jdbc.jar DuckDbUi.java");
            System.exit(1);
        }
        Properties props = new Properties();
        if (readOnly) props.setProperty("duckdb.read_only", "true");
        if (db != null) dbLabel = db;
        base = DriverManager.getConnection("jdbc:duckdb:" + (db == null ? "" : db), props);
        slots = new Semaphore(MAX_CONNECTIONS, true);
        applyGlobalSettings();
        try (Statement st = base.createStatement(); ResultSet rs = st.executeQuery("SELECT version()")) {
            if (rs.next()) version = rs.getString(1);
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/", ex -> {
            try {
                route(ex);
            } catch (Throwable t) {
                try { json(ex, 500, "{\"error\":" + q(String.valueOf(t)) + "}"); } catch (Throwable ignore) { }
            } finally {
                ex.close();
            }
        });
        server.setExecutor(Executors.newFixedThreadPool(MAX_CONNECTIONS + 6));
        server.start();
        System.out.println("DuckDB " + version + " | database: " + dbLabel + (readOnly ? " (read-only)" : ""));
        System.out.println("Settings: memory_limit=" + (MEMORY_LIMIT.isEmpty() ? "default" : MEMORY_LIMIT) + ", threads=" + (THREADS > 0 ? THREADS : "default")
                + ", temp_directory=" + (TEMP_DIRECTORY.isEmpty() ? "default" : TEMP_DIRECTORY) + ", preserve_insertion_order=" + PRESERVE_INSERTION_ORDER
                + ", max_connections=" + MAX_CONNECTIONS);
        System.out.println("Listening on http://" + host + ":" + port + (expectedAuth != null ? "  (basic auth enabled)" : "  (NO AUTH)"));
        System.out.println("Press Ctrl+C to stop.");
    }

    // ------------------------------------------------------------------ connections & global settings

    /** Applies the CONFIG block. Each SET is independent so one unsupported setting on an older DuckDB doesn't stop startup. */
    static void applyGlobalSettings() {
        List<String> sets = new ArrayList<>();
        if (!MEMORY_LIMIT.isEmpty()) sets.add("SET memory_limit=" + sqlStr(MEMORY_LIMIT));
        if (THREADS > 0) sets.add("SET threads=" + THREADS);
        if (!TEMP_DIRECTORY.isEmpty()) {
            try { Files.createDirectories(Paths.get(TEMP_DIRECTORY)); } catch (IOException e) { System.err.println("WARN cannot create temp dir " + TEMP_DIRECTORY + ": " + e); }
            sets.add("SET temp_directory=" + sqlStr(TEMP_DIRECTORY));
        }
        if (!MAX_TEMP_DIR_SIZE.isEmpty()) sets.add("SET max_temp_directory_size=" + sqlStr(MAX_TEMP_DIR_SIZE));
        sets.add("SET preserve_insertion_order=" + PRESERVE_INSERTION_ORDER);
        sets.addAll(Arrays.asList(EXTRA_SETTINGS));
        for (String s : sets) {
            try (Statement st = base.createStatement()) { st.execute(s); }
            catch (SQLException e) { System.err.println("WARN setting failed [" + s + "]: " + e.getMessage()); }
        }
    }

    static String sqlStr(String s) { return "'" + s.replace("'", "''") + "'"; }

    /** Waits for one of MAX_CONNECTIONS slots and returns a fresh connection to the same database instance. */
    static Connection borrow() throws Exception {
        if (!slots.tryAcquire(QUEUE_WAIT_SECONDS, TimeUnit.SECONDS)) {
            throw new TooBusy("All " + MAX_CONNECTIONS + " query slots are busy. Try again shortly.");
        }
        try {
            return (Connection) base.getClass().getMethod("duplicate").invoke(base);
        } catch (Throwable t) {
            slots.release();
            throw t instanceof Exception ? (Exception) t : new RuntimeException(t);
        }
    }

    static void giveBack(Connection c) {
        try { if (c != null) c.close(); } catch (SQLException ignore) { }
        slots.release();
    }

    static class TooBusy extends Exception { TooBusy(String m) { super(m); } }

    // ------------------------------------------------------------------ routing

    static void route(HttpExchange ex) throws Exception {
        if (expectedAuth != null && !expectedAuth.equals(ex.getRequestHeaders().getFirst("Authorization"))) {
            ex.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"DuckDB Console\"");
            json(ex, 401, "{\"error\":\"authentication required\"}");
            return;
        }
        String path = ex.getRequestURI().getPath();
        String method = ex.getRequestMethod();
        if (method.equals("GET") && path.equals("/")) {
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.getResponseHeaders().set("Cache-Control", "no-store");
            ex.sendResponseHeaders(200, page.length);
            ex.getResponseBody().write(page);
        } else if (method.equals("GET") && path.equals("/api/info")) {
            json(ex, 200, "{\"version\":" + q(version) + ",\"db\":" + q(dbLabel) + ",\"readonly\":" + readOnly
                    + ",\"root\":" + q(root.toString()) + ",\"start\":" + q(startDir().toString())
                    + ",\"java\":" + q(System.getProperty("java.version")) + "}");
        } else if (method.equals("GET") && path.equals("/api/files")) {
            files(ex);
        } else if (method.equals("POST") && path.equals("/api/query")) {
            query(ex);
        } else if (method.equals("POST") && path.equals("/api/export")) {
            export(ex);
        } else if (method.equals("POST") && path.equals("/api/cancel")) {
            String qid = param(ex, "qid");
            Statement s = qid == null ? null : RUNNING.get(qid);
            if (s != null) { try { s.cancel(); } catch (SQLException ignore) { } }
            json(ex, 200, "{\"ok\":true}");
        } else {
            json(ex, 404, "{\"error\":\"not found\"}");
        }
    }

    static Path startDir() {
        Path cwd = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        return cwd.startsWith(root) ? cwd : root;
    }

    // ------------------------------------------------------------------ query

    static void query(HttpExchange ex) throws Exception {
        String sql = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        int limit = Math.max(1, Math.min(1_000_000, intParam(ex, "limit", 1000)));
        String qid = param(ex, "qid");
        StringBuilder sb = new StringBuilder();
        int status = 200;
        Connection c;
        try { c = borrow(); } catch (TooBusy e) { json(ex, 503, "{\"error\":" + q(e.getMessage()) + "}"); return; }
        try (Statement st = c.createStatement()) {
            long t0 = System.nanoTime();
            if (qid != null) RUNNING.put(qid, st);
            if (st.execute(sql)) {
                try (ResultSet rs = st.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    int n = md.getColumnCount();
                    sb.append("{\"cols\":[");
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) sb.append(',');
                        sb.append("{\"n\":").append(q(md.getColumnLabel(i))).append(",\"t\":").append(q(md.getColumnTypeName(i))).append('}');
                    }
                    sb.append("],\"rows\":[");
                    int count = 0;
                    boolean truncated = false;
                    while (rs.next()) {
                        if (count == limit) { truncated = true; break; }
                        if (count++ > 0) sb.append(',');
                        sb.append('[');
                        for (int i = 1; i <= n; i++) {
                            if (i > 1) sb.append(',');
                            String v = rs.getString(i);
                            sb.append(v == null ? "null" : q(v));
                        }
                        sb.append(']');
                    }
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    sb.append("],\"truncated\":").append(truncated).append(",\"ms\":").append(ms).append('}');
                }
            } else {
                long ms = (System.nanoTime() - t0) / 1_000_000;
                sb.append("{\"update\":").append(st.getUpdateCount()).append(",\"ms\":").append(ms).append('}');
            }
        } catch (SQLException e) {
            status = 400;
            sb.setLength(0);
            sb.append("{\"error\":").append(q(e.getMessage())).append('}');
        } finally {
            if (qid != null) RUNNING.remove(qid);
            giveBack(c);
        }
        json(ex, status, sb.toString());
    }

    /** Streams the complete (un-truncated) result as CSV. */
    static void export(HttpExchange ex) throws Exception {
        String sql = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Connection c;
        try { c = borrow(); } catch (TooBusy e) { json(ex, 503, "{\"error\":" + q(e.getMessage()) + "}"); return; }
        try (Statement st = c.createStatement()) {
            if (!st.execute(sql)) {
                json(ex, 400, "{\"error\":\"statement did not return a result set\"}");
                return;
            }
            try (ResultSet rs = st.getResultSet()) {
                ex.getResponseHeaders().set("Content-Type", "text/csv; charset=utf-8");
                ex.getResponseHeaders().set("Content-Disposition", "attachment; filename=\"result.csv\"");
                ex.sendResponseHeaders(200, 0); // chunked
                try (Writer w = new BufferedWriter(new OutputStreamWriter(ex.getResponseBody(), StandardCharsets.UTF_8), 1 << 16)) {
                    w.write('\uFEFF'); // BOM so Excel reads UTF-8 correctly
                    ResultSetMetaData md = rs.getMetaData();
                    int n = md.getColumnCount();
                    for (int i = 1; i <= n; i++) { if (i > 1) w.write(','); w.write(csv(md.getColumnLabel(i))); }
                    w.write("\r\n");
                    while (rs.next()) {
                        for (int i = 1; i <= n; i++) { if (i > 1) w.write(','); String v = rs.getString(i); w.write(v == null ? "" : csv(v)); }
                        w.write("\r\n");
                    }
                }
            }
        } catch (SQLException e) {
            json(ex, 400, "{\"error\":" + q(e.getMessage()) + "}");
        } finally {
            giveBack(c);
        }
    }

    static String csv(String s) {
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0 && s.indexOf('\r') < 0) return s;
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    // ------------------------------------------------------------------ file browser

    static void files(HttpExchange ex) throws Exception {
        String p = param(ex, "path");
        Path dir = (p == null || p.isEmpty()) ? startDir() : Paths.get(p).toAbsolutePath().normalize();
        if (!dir.startsWith(root)) { json(ex, 403, "{\"error\":\"outside allowed root " + esc(root.toString()) + "\"}"); return; }
        if (!Files.isDirectory(dir)) { json(ex, 400, "{\"error\":\"not a directory: " + esc(dir.toString()) + "\"}"); return; }
        List<Path> list = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path e : ds) list.add(e);
        } catch (IOException e) {
            json(ex, 400, "{\"error\":" + q("cannot read " + dir + ": " + e) + "}");
            return;
        }
        list.sort((x, y) -> {
            boolean dx = Files.isDirectory(x), dy = Files.isDirectory(y);
            if (dx != dy) return dx ? -1 : 1;
            return x.getFileName().toString().compareToIgnoreCase(y.getFileName().toString());
        });
        StringBuilder sb = new StringBuilder("{\"path\":").append(q(dir.toString())).append(",\"parent\":");
        Path parent = dir.getParent();
        sb.append(parent != null && parent.startsWith(root) ? q(parent.toString()) : "null").append(",\"entries\":[");
        boolean first = true;
        for (Path e : list) {
            boolean d = Files.isDirectory(e);
            long size = 0;
            try { if (!d) size = Files.size(e); } catch (IOException ignore) { }
            if (!first) sb.append(',');
            first = false;
            sb.append("{\"n\":").append(q(e.getFileName().toString())).append(",\"d\":").append(d).append(",\"s\":").append(size).append('}');
        }
        sb.append("]}");
        json(ex, 200, sb.toString());
    }

    // ------------------------------------------------------------------ helpers

    static String param(HttpExchange ex, String key) throws UnsupportedEncodingException {
        String qs = ex.getRequestURI().getRawQuery();
        if (qs == null) return null;
        for (String kv : qs.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0 && URLDecoder.decode(kv.substring(0, i), "UTF-8").equals(key)) return URLDecoder.decode(kv.substring(i + 1), "UTF-8");
        }
        return null;
    }

    static int intParam(HttpExchange ex, String key, int def) {
        try { String v = param(ex, key); return v == null ? def : Integer.parseInt(v); } catch (Exception e) { return def; }
    }

    static void json(HttpExchange ex, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, b.length);
        ex.getResponseBody().write(b);
    }

    /** JSON string literal. */
    static String q(String s) { return '"' + esc(s) + '"'; }

    static String esc(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c);
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ embedded UI (base64 of index.html)

    static final String[] HTML_B64 = {
        "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImVuIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPHRpdGxlPkR1Y2tE",
        "QiBDb25zb2xlPC90aXRsZT4KPG1ldGEgbmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCwgaW5pdGlh",
        "bC1zY2FsZT0xIj4KPHN0eWxlPgo6cm9vdHsKICAtLWJnOiNmNmY3Zjk7IC0tcGFuZWw6I2ZmZmZmZjsgLS1pbms6IzFjMjEyODsg",
        "LS1kaW06IzZiNzQ4MDsgLS1saW5lOiNkZGUxZTY7CiAgLS1hY2NlbnQ6I2U2YTcwMDsgLS1hY2NlbnQtaW5rOiMxYzE0MDA7IC0t",
        "ZXJyOiNiMzI2MWU7IC0tb2s6IzFiN2YzYjsgLS1ob3ZlcjojZWVmMWY1OyAtLWNvZGU6I2ZiZmJmYzsKfQpAbWVkaWEgKHByZWZl",
        "cnMtY29sb3Itc2NoZW1lOiBkYXJrKXsKICA6cm9vdHsgLS1iZzojMTQxNzFiOyAtLXBhbmVsOiMxYjFmMjQ7IC0taW5rOiNlNmU5",
        "ZWQ7IC0tZGltOiM4Yjk1YTE7IC0tbGluZTojMmMzMzNiOwogICAgLS1hY2NlbnQ6I2YyYjcwNTsgLS1hY2NlbnQtaW5rOiMxYzE0",
        "MDA7IC0tZXJyOiNmZjZiNjI7IC0tb2s6IzRjYzM2ZTsgLS1ob3ZlcjojMjQyYTMxOyAtLWNvZGU6IzE3MWIyMDsgfQp9Cip7Ym94",
        "LXNpemluZzpib3JkZXItYm94fQpodG1sLGJvZHl7aGVpZ2h0OjEwMCU7bWFyZ2luOjB9CmJvZHl7YmFja2dyb3VuZDp2YXIoLS1i",
        "Zyk7Y29sb3I6dmFyKC0taW5rKTtmb250OjEzcHgvMS40IHN5c3RlbS11aSwtYXBwbGUtc3lzdGVtLCJTZWdvZSBVSSIsUm9ib3Rv",
        "LHNhbnMtc2VyaWY7ZGlzcGxheTpmbGV4O2ZsZXgtZGlyZWN0aW9uOmNvbHVtbn0KaGVhZGVye2Rpc3BsYXk6ZmxleDthbGlnbi1p",
        "dGVtczpjZW50ZXI7Z2FwOjEycHg7cGFkZGluZzo4cHggMTRweDtiYWNrZ3JvdW5kOnZhcigtLXBhbmVsKTtib3JkZXItYm90dG9t",
        "OjFweCBzb2xpZCB2YXIoLS1saW5lKX0KaGVhZGVyIGgxe2ZvbnQtc2l6ZToxNHB4O21hcmdpbjowO2ZvbnQtd2VpZ2h0OjY1MH0K",
        "aGVhZGVyIC5tZXRhe2NvbG9yOnZhcigtLWRpbSk7Zm9udC1zaXplOjEycHg7d2hpdGUtc3BhY2U6bm93cmFwO292ZXJmbG93Omhp",
        "ZGRlbjt0ZXh0LW92ZXJmbG93OmVsbGlwc2lzfQouZG90e3dpZHRoOjEwcHg7aGVpZ2h0OjEwcHg7Ym9yZGVyLXJhZGl1czo1MCU7",
        "YmFja2dyb3VuZDp2YXIoLS1hY2NlbnQpO2ZsZXg6bm9uZX0KbWFpbntmbGV4OjE7ZGlzcGxheTpmbGV4O21pbi1oZWlnaHQ6MH0K",
        "YXNpZGV7d2lkdGg6MjkwcHg7ZmxleDpub25lO2JhY2tncm91bmQ6dmFyKC0tcGFuZWwpO2JvcmRlci1yaWdodDoxcHggc29saWQg",
        "dmFyKC0tbGluZSk7ZGlzcGxheTpmbGV4O2ZsZXgtZGlyZWN0aW9uOmNvbHVtbjttaW4taGVpZ2h0OjB9Ci50YWJze2Rpc3BsYXk6",
        "ZmxleDtib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKX0KLnRhYnMgYnV0dG9ue2ZsZXg6MTtib3JkZXI6MDtiYWNr",
        "Z3JvdW5kOm5vbmU7Y29sb3I6dmFyKC0tZGltKTtwYWRkaW5nOjlweCA0cHg7Zm9udDppbmhlcml0O2N1cnNvcjpwb2ludGVyO2Jv",
        "cmRlci1ib3R0b206MnB4IHNvbGlkIHRyYW5zcGFyZW50fQoudGFicyBidXR0b24ub257Y29sb3I6dmFyKC0taW5rKTtib3JkZXIt",
        "Ym90dG9tLWNvbG9yOnZhcigtLWFjY2VudCk7Zm9udC13ZWlnaHQ6NjAwfQoucGFuZXtkaXNwbGF5Om5vbmU7ZmxleDoxO21pbi1o",
        "ZWlnaHQ6MDtmbGV4LWRpcmVjdGlvbjpjb2x1bW59Ci5wYW5lLm9ue2Rpc3BsYXk6ZmxleH0KLnBhdGhiYXJ7ZGlzcGxheTpmbGV4",
        "O2dhcDo0cHg7cGFkZGluZzo4cHh9Ci5wYXRoYmFyIGlucHV0e2ZsZXg6MTttaW4td2lkdGg6MH0KaW5wdXQsc2VsZWN0LHRleHRh",
        "cmVhLGJ1dHRvbntmb250OmluaGVyaXQ7Y29sb3I6aW5oZXJpdH0KaW5wdXQsc2VsZWN0e2JhY2tncm91bmQ6dmFyKC0tY29kZSk7",
        "Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjZweDtwYWRkaW5nOjVweCA4cHh9CmJ1dHRvbi5idG57",
        "YmFja2dyb3VuZDp2YXIoLS1wYW5lbCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjZweDtwYWRk",
        "aW5nOjVweCAxMHB4O2N1cnNvcjpwb2ludGVyfQpidXR0b24uYnRuOmhvdmVyOm5vdCg6ZGlzYWJsZWQpe2JhY2tncm91bmQ6dmFy",
        "KC0taG92ZXIpfQpidXR0b24uYnRuOmRpc2FibGVke29wYWNpdHk6LjU7Y3Vyc29yOmRlZmF1bHR9CmJ1dHRvbi5wcmltYXJ5e2Jh",
        "Y2tncm91bmQ6dmFyKC0tYWNjZW50KTtib3JkZXItY29sb3I6dmFyKC0tYWNjZW50KTtjb2xvcjp2YXIoLS1hY2NlbnQtaW5rKTtm",
        "b250LXdlaWdodDo2NTB9CmJ1dHRvbi5wcmltYXJ5OmhvdmVyOm5vdCg6ZGlzYWJsZWQpe2JhY2tncm91bmQ6dmFyKC0tYWNjZW50",
        "KTtmaWx0ZXI6YnJpZ2h0bmVzcygxLjA3KX0KLmxpc3R7ZmxleDoxO292ZXJmbG93OmF1dG87cGFkZGluZzowIDAgOHB4fQoucm93",
        "e2Rpc3BsYXk6ZmxleDtnYXA6OHB4O2FsaWduLWl0ZW1zOmJhc2VsaW5lO3BhZGRpbmc6NHB4IDEwcHg7Y3Vyc29yOnBvaW50ZXJ9",
        "Ci5yb3c6aG92ZXJ7YmFja2dyb3VuZDp2YXIoLS1ob3Zlcil9Ci5yb3cgLm57ZmxleDoxO21pbi13aWR0aDowO292ZXJmbG93Omhp",
        "ZGRlbjt0ZXh0LW92ZXJmbG93OmVsbGlwc2lzO3doaXRlLXNwYWNlOm5vd3JhcH0KLnJvdyAuc3tjb2xvcjp2YXIoLS1kaW0pO2Zv",
        "bnQtc2l6ZToxMXB4O2ZsZXg6bm9uZX0KLnJvdy5kaXIgLm57Zm9udC13ZWlnaHQ6NjAwfQouaGludHtjb2xvcjp2YXIoLS1kaW0p",
        "O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHh9Ci5pdGVte3BhZGRpbmc6N3B4IDEwcHg7Ym9yZGVyLWJvdHRvbToxcHgg",
        "c29saWQgdmFyKC0tbGluZSk7Y3Vyc29yOnBvaW50ZXJ9Ci5pdGVtOmhvdmVye2JhY2tncm91bmQ6dmFyKC0taG92ZXIpfQouaXRl",
        "bSBwcmV7bWFyZ2luOjA7d2hpdGUtc3BhY2U6cHJlLXdyYXA7d29yZC1icmVhazpicmVhay13b3JkO2ZvbnQ6MTJweC8xLjM1IHVp",
        "LW1vbm9zcGFjZSxNZW5sbyxDb25zb2xhcyxtb25vc3BhY2U7bWF4LWhlaWdodDo0LjFlbTtvdmVyZmxvdzpoaWRkZW59Ci5pdGVt",
        "IHNtYWxse2NvbG9yOnZhcigtLWRpbSk7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo2cHg7",
        "bWFyZ2luLXRvcDozcHh9Ci5pdGVtIC54e2JhY2tncm91bmQ6bm9uZTtib3JkZXI6MDtjb2xvcjp2YXIoLS1kaW0pO2N1cnNvcjpw",
        "b2ludGVyO3BhZGRpbmc6MCA0cHh9Ci5pdGVtIC54OmhvdmVye2NvbG9yOnZhcigtLWVycil9CnNlY3Rpb24ud29ya3tmbGV4OjE7",
        "bWluLXdpZHRoOjA7ZGlzcGxheTpmbGV4O2ZsZXgtZGlyZWN0aW9uOmNvbHVtbjtwYWRkaW5nOjEwcHg7Z2FwOjhweH0KI3NxbHt3",
        "aWR0aDoxMDAlO2hlaWdodDoyMDBweDttaW4taGVpZ2h0OjgwcHg7cmVzaXplOnZlcnRpY2FsO2JhY2tncm91bmQ6dmFyKC0tY29k",
        "ZSk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjhweDtwYWRkaW5nOjEwcHg7Zm9udDoxM3B4LzEu",
        "NDUgdWktbW9ub3NwYWNlLE1lbmxvLENvbnNvbGFzLG1vbm9zcGFjZTt0YWItc2l6ZToyfQojc3FsOmZvY3Vze291dGxpbmU6MnB4",
        "IHNvbGlkIHZhcigtLWFjY2VudCk7b3V0bGluZS1vZmZzZXQ6LTFweH0KLmJhcntkaXNwbGF5OmZsZXg7Z2FwOjhweDthbGlnbi1p",
        "dGVtczpjZW50ZXI7ZmxleC13cmFwOndyYXB9Ci5iYXIgLnNwe2ZsZXg6MX0KLnN0YXR1c3tjb2xvcjp2YXIoLS1kaW0pO2ZvbnQt",
        "c2l6ZToxMnB4fQouc3RhdHVzLmVycntjb2xvcjp2YXIoLS1lcnIpO3doaXRlLXNwYWNlOnByZS13cmFwfQouc3RhdHVzLm9re2Nv",
        "bG9yOnZhcigtLW9rKX0KLmdyaWR7ZmxleDoxO21pbi1oZWlnaHQ6MDtvdmVyZmxvdzphdXRvO2JvcmRlcjoxcHggc29saWQgdmFy",
        "KC0tbGluZSk7Ym9yZGVyLXJhZGl1czo4cHg7YmFja2dyb3VuZDp2YXIoLS1wYW5lbCl9CnRhYmxle2JvcmRlci1jb2xsYXBzZTpz",
        "ZXBhcmF0ZTtib3JkZXItc3BhY2luZzowO2ZvbnQ6MTJweC8xLjM1IHVpLW1vbm9zcGFjZSxNZW5sbyxDb25zb2xhcyxtb25vc3Bh",
        "Y2V9CnRoLHRke3BhZGRpbmc6NHB4IDEwcHg7Ym9yZGVyLWJvdHRvbToxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJpZ2h0",
        "OjFweCBzb2xpZCB2YXIoLS1saW5lKTt3aGl0ZS1zcGFjZTpub3dyYXA7bWF4LXdpZHRoOjQyMHB4O292ZXJmbG93OmhpZGRlbjt0",
        "ZXh0LW92ZXJmbG93OmVsbGlwc2lzO3RleHQtYWxpZ246bGVmdH0KdGh7cG9zaXRpb246c3RpY2t5O3RvcDowO2JhY2tncm91bmQ6",
        "dmFyKC0taG92ZXIpO2N1cnNvcjpwb2ludGVyO3VzZXItc2VsZWN0Om5vbmU7ei1pbmRleDoxO2ZvbnQtd2VpZ2h0OjY1MH0KdGgg",
        "c21hbGx7Y29sb3I6dmFyKC0tZGltKTtmb250LXdlaWdodDo0MDA7bWFyZ2luLWxlZnQ6NnB4fQp0aCAuYXJ7Y29sb3I6dmFyKC0t",
        "YWNjZW50KX0KdGQubnVte3RleHQtYWxpZ246cmlnaHR9CnRkLm51bGx7Y29sb3I6dmFyKC0tZGltKTtmb250LXN0eWxlOml0YWxp",
        "Y30KdGQuaWR4LHRoLmlkeHtjb2xvcjp2YXIoLS1kaW0pO3RleHQtYWxpZ246cmlnaHQ7YmFja2dyb3VuZDp2YXIoLS1ob3Zlcik7",
        "cG9zaXRpb246c3RpY2t5O2xlZnQ6MH0KdGguaWR4e3otaW5kZXg6Mn0KdHI6aG92ZXIgdGQ6bm90KC5pZHgpe2JhY2tncm91bmQ6",
        "dmFyKC0taG92ZXIpfQouZW1wdHl7cGFkZGluZzoyNHB4O2NvbG9yOnZhcigtLWRpbSl9CmtiZHtib3JkZXI6MXB4IHNvbGlkIHZh",
        "cigtLWxpbmUpO2JvcmRlci1ib3R0b20td2lkdGg6MnB4O2JvcmRlci1yYWRpdXM6NHB4O3BhZGRpbmc6MCA0cHg7Zm9udC1zaXpl",
        "OjExcHg7Y29sb3I6dmFyKC0tZGltKX0KPC9zdHlsZT4KPC9oZWFkPgo8Ym9keT4KPGhlYWRlcj4KICA8c3BhbiBjbGFzcz0iZG90",
        "Ij48L3NwYW4+CiAgPGgxPkR1Y2tEQiBDb25zb2xlPC9oMT4KICA8c3BhbiBjbGFzcz0ibWV0YSIgaWQ9Im1ldGEiPmNvbm5lY3Rp",
        "bmfigKY8L3NwYW4+CjwvaGVhZGVyPgo8bWFpbj4KICA8YXNpZGU+CiAgICA8ZGl2IGNsYXNzPSJ0YWJzIj4KICAgICAgPGJ1dHRv",
        "biBkYXRhLXRhYj0iZmlsZXMiIGNsYXNzPSJvbiI+RmlsZXM8L2J1dHRvbj4KICAgICAgPGJ1dHRvbiBkYXRhLXRhYj0iaGlzdG9y",
        "eSI+SGlzdG9yeTwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdGFiPSJzYXZlZCI+U2F2ZWQ8L2J1dHRvbj4KICAgIDwvZGl2",
        "PgogICAgPGRpdiBjbGFzcz0icGFuZSBvbiIgaWQ9InBhbmUtZmlsZXMiPgogICAgICA8ZGl2IGNsYXNzPSJwYXRoYmFyIj4KICAg",
        "ICAgICA8YnV0dG9uIGNsYXNzPSJidG4iIGlkPSJ1cCIgdGl0bGU9IlBhcmVudCBmb2xkZXIiPiZ1YXJyOzwvYnV0dG9uPgogICAg",
        "ICAgIDxpbnB1dCBpZD0icGF0aCIgc3BlbGxjaGVjaz0iZmFsc2UiIHBsYWNlaG9sZGVyPSIvcGF0aC9vbi9zZXJ2ZXIiPgogICAg",
        "ICAgIDxidXR0b24gY2xhc3M9ImJ0biIgaWQ9ImdvIj5HbzwvYnV0dG9uPgogICAgICA8L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0i",
        "aGludCI+Q2xpY2sgYSBmaWxlIHRvIGluc2VydCBhIHF1ZXJ5IGZvciBpdCBhdCB0aGUgY3Vyc29yLjwvZGl2PgogICAgICA8ZGl2",
        "IGNsYXNzPSJsaXN0IiBpZD0iZmlsZXMiPjwvZGl2PgogICAgPC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJwYW5lIiBpZD0icGFuZS1o",
        "aXN0b3J5Ij48ZGl2IGNsYXNzPSJsaXN0IiBpZD0iaGlzdG9yeSI+PC9kaXY+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJwYW5lIiBp",
        "ZD0icGFuZS1zYXZlZCI+PGRpdiBjbGFzcz0ibGlzdCIgaWQ9InNhdmVkIj48L2Rpdj48L2Rpdj4KICA8L2FzaWRlPgogIDxzZWN0",
        "aW9uIGNsYXNzPSJ3b3JrIj4KICAgIDx0ZXh0YXJlYSBpZD0ic3FsIiBzcGVsbGNoZWNrPSJmYWxzZSIgcGxhY2Vob2xkZXI9IlNF",
        "TEVDVCAqIEZST00gcmVhZF9wYXJxdWV0KCcvZGF0YS9maWxlLnBhcnF1ZXQnKSBMSU1JVCAxMDA7Ij48L3RleHRhcmVhPgogICAg",
        "PGRpdiBjbGFzcz0iYmFyIj4KICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIHByaW1hcnkiIGlkPSJydW4iPlJ1bjwvYnV0dG9uPgog",
        "ICAgICA8YnV0dG9uIGNsYXNzPSJidG4iIGlkPSJjYW5jZWwiIGRpc2FibGVkPkNhbmNlbDwvYnV0dG9uPgogICAgICA8bGFiZWwg",
        "Y2xhc3M9InN0YXR1cyI+TWF4IHJvd3MKICAgICAgICA8c2VsZWN0IGlkPSJsaW1pdCI+PG9wdGlvbj4xMDA8L29wdGlvbj48b3B0",
        "aW9uIHNlbGVjdGVkPjEwMDA8L29wdGlvbj48b3B0aW9uPjEwMDAwPC9vcHRpb24+PG9wdGlvbj4xMDAwMDA8L29wdGlvbj48L3Nl",
        "bGVjdD4KICAgICAgPC9sYWJlbD4KICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBpZD0iZXhwb3J0IiBkaXNhYmxlZD5FeHBvcnQg",
        "Q1NWPC9idXR0b24+CiAgICAgIDxidXR0b24gY2xhc3M9ImJ0biIgaWQ9InNhdmUiPlNhdmUgcXVlcnk8L2J1dHRvbj4KICAgICAg",
        "PHNwYW4gY2xhc3M9InNwIj48L3NwYW4+CiAgICAgIDxzcGFuIGNsYXNzPSJzdGF0dXMiPjxrYmQ+Q3RybDwva2JkPis8a2JkPkVu",
        "dGVyPC9rYmQ+IHJ1bnMgdGhlIHNlbGVjdGlvbiwgb3IgZXZlcnl0aGluZzwvc3Bhbj4KICAgIDwvZGl2PgogICAgPGRpdiBjbGFz",
        "cz0ic3RhdHVzIiBpZD0ic3RhdHVzIj5SZWFkeS48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImdyaWQiIGlkPSJncmlkIj48ZGl2IGNs",
        "YXNzPSJlbXB0eSI+UmVzdWx0cyBhcHBlYXIgaGVyZS48L2Rpdj48L2Rpdj4KICA8L3NlY3Rpb24+CjwvbWFpbj4KPHNjcmlwdD4K",
        "Y29uc3QgJCA9IHMgPT4gZG9jdW1lbnQucXVlcnlTZWxlY3RvcihzKTsKY29uc3QgZXNjID0gcyA9PiBTdHJpbmcocykucmVwbGFj",
        "ZSgvWyY8PiJdL2csIGMgPT4gKHsnJic6JyZhbXA7JywnPCc6JyZsdDsnLCc+JzonJmd0OycsJyInOicmcXVvdDsnfVtjXSkpOwpj",
        "b25zdCBlZCA9ICQoJyNzcWwnKTsKbGV0IGN1clFpZCA9ICcnLCBsYXN0U3FsID0gJycsIGNvbHMgPSBbXSwgcm93cyA9IFtdLCBz",
        "b3J0Q29sID0gLTEsIHNvcnREaXIgPSAxLCBjd2QgPSAnJzsKCi8qIC0tLS0gYnJvd3NlciBzdG9yYWdlIChiZXN0IGVmZm9ydCkg",
        "LS0tLSAqLwpjb25zdCBzdG9yZSA9IHsKICBnZXQoaywgZCl7IHRyeSB7IHJldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5n",
        "ZXRJdGVtKGspKSA/PyBkOyB9IGNhdGNoKGUpeyByZXR1cm4gZDsgfSB9LAogIHNldChrLCB2KXsgdHJ5IHsgbG9jYWxTdG9yYWdl",
        "LnNldEl0ZW0oaywgSlNPTi5zdHJpbmdpZnkodikpOyB9IGNhdGNoKGUpe30gfQp9OwoKLyogLS0tLSB0YWJzIC0tLS0gKi8KZG9j",
        "dW1lbnQucXVlcnlTZWxlY3RvckFsbCgnLnRhYnMgYnV0dG9uJykuZm9yRWFjaChiID0+IGIub25jbGljayA9ICgpID0+IHsKICBk",
        "b2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKCcudGFicyBidXR0b24nKS5mb3JFYWNoKHggPT4geC5jbGFzc0xpc3QudG9nZ2xlKCdv",
        "bicsIHggPT09IGIpKTsKICBkb2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKCcucGFuZScpLmZvckVhY2gocCA9PiBwLmNsYXNzTGlz",
        "dC50b2dnbGUoJ29uJywgcC5pZCA9PT0gJ3BhbmUtJyArIGIuZGF0YXNldC50YWIpKTsKfSk7CgovKiAtLS0tIGFwaSAtLS0tICov",
        "CmFzeW5jIGZ1bmN0aW9uIGFwaSh1cmwsIG9wdHMpewogIGNvbnN0IHIgPSBhd2FpdCBmZXRjaCh1cmwsIG9wdHMpOwogIGxldCBq",
        "OyB0cnkgeyBqID0gYXdhaXQgci5qc29uKCk7IH0gY2F0Y2goZSl7IHRocm93IG5ldyBFcnJvcignSFRUUCAnICsgci5zdGF0dXMp",
        "OyB9CiAgaWYgKCFyLm9rIHx8IGouZXJyb3IpIHRocm93IG5ldyBFcnJvcihqLmVycm9yIHx8ICgnSFRUUCAnICsgci5zdGF0dXMp",
        "KTsKICByZXR1cm4gajsKfQoKLyogLS0tLSBydW5uaW5nIHF1ZXJpZXMgLS0tLSAqLwpmdW5jdGlvbiBzZXRCdXN5KGIpewogICQo",
        "JyNydW4nKS5kaXNhYmxlZCA9IGI7ICQoJyNjYW5jZWwnKS5kaXNhYmxlZCA9ICFiOwogIGlmIChiKSAkKCcjZXhwb3J0JykuZGlz",
        "YWJsZWQgPSB0cnVlOwp9CmZ1bmN0aW9uIHN0YXR1cyhtc2csIGNscyl7IGNvbnN0IHMgPSAkKCcjc3RhdHVzJyk7IHMudGV4dENv",
        "bnRlbnQgPSBtc2c7IHMuY2xhc3NOYW1lID0gJ3N0YXR1cyAnICsgKGNscyB8fCAnJyk7IH0KZnVuY3Rpb24gY3VycmVudFNxbCgp",
        "ewogIGNvbnN0IHNlbCA9IGVkLnZhbHVlLnN1YnN0cmluZyhlZC5zZWxlY3Rpb25TdGFydCwgZWQuc2VsZWN0aW9uRW5kKS50cmlt",
        "KCk7CiAgcmV0dXJuIHNlbCB8fCBlZC52YWx1ZS50cmltKCk7Cn0KYXN5bmMgZnVuY3Rpb24gcnVuKCl7CiAgY29uc3Qgc3FsID0g",
        "Y3VycmVudFNxbCgpOwogIGlmICghc3FsKSByZXR1cm47CiAgbGFzdFNxbCA9IHNxbDsgc2V0QnVzeSh0cnVlKTsgc3RhdHVzKCdS",
        "dW5uaW5n4oCmJyk7CiAgY29uc3QgdDAgPSBwZXJmb3JtYW5jZS5ub3coKTsKICB0cnkgewogICAgY3VyUWlkID0gRGF0ZS5ub3co",
        "KS50b1N0cmluZygzNikgKyBNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zbGljZSgyKTsKICAgIGNvbnN0IGogPSBhd2FpdCBh",
        "cGkoJy9hcGkvcXVlcnk/bGltaXQ9JyArICQoJyNsaW1pdCcpLnZhbHVlICsgJyZxaWQ9JyArIGN1clFpZCwge21ldGhvZDonUE9T",
        "VCcsIGJvZHk6IHNxbH0pOwogICAgY29uc3Qgd2FsbCA9IE1hdGgucm91bmQocGVyZm9ybWFuY2Uubm93KCkgLSB0MCk7CiAgICBp",
        "ZiAoai5jb2xzKXsKICAgICAgY29scyA9IGouY29sczsgcm93cyA9IGoucm93czsgc29ydENvbCA9IC0xOwogICAgICByZW5kZXJH",
        "cmlkKCk7CiAgICAgIHN0YXR1cyhyb3dzLmxlbmd0aC50b0xvY2FsZVN0cmluZygpICsgJyByb3cnICsgKHJvd3MubGVuZ3RoID09",
        "PSAxID8gJycgOiAncycpICsKICAgICAgICAoai50cnVuY2F0ZWQgPyAnIChtb3JlIGF2YWlsYWJsZSDigJQgcmFpc2UgTWF4IHJv",
        "d3MsIG9yIHVzZSBFeHBvcnQgQ1NWIGZvciBldmVyeXRoaW5nKScgOiAnJykgKwogICAgICAgICcgwrcgJyArIGoubXMgKyAnIG1z",
        "IHF1ZXJ5LCAnICsgd2FsbCArICcgbXMgdG90YWwnLCAnb2snKTsKICAgICAgJCgnI2V4cG9ydCcpLmRpc2FibGVkID0gZmFsc2U7",
        "CiAgICB9IGVsc2UgewogICAgICBjb2xzID0gW107IHJvd3MgPSBbXTsKICAgICAgJCgnI2dyaWQnKS5pbm5lckhUTUwgPSAnPGRp",
        "diBjbGFzcz0iZW1wdHkiPlN0YXRlbWVudCBjb21wbGV0ZWQuIFJvd3MgYWZmZWN0ZWQ6ICcgKyBlc2Moai51cGRhdGUpICsgJzwv",
        "ZGl2Pic7CiAgICAgIHN0YXR1cygnRG9uZSDCtyAnICsgai5tcyArICcgbXMnLCAnb2snKTsKICAgIH0KICAgIGFkZEhpc3Rvcnko",
        "c3FsLCBqLm1zLCBqLmNvbHMgPyByb3dzLmxlbmd0aCA6IG51bGwsIHRydWUpOwogIH0gY2F0Y2goZSl7CiAgICBzdGF0dXMoZS5t",
        "ZXNzYWdlLCAnZXJyJyk7CiAgICBhZGRIaXN0b3J5KHNxbCwgbnVsbCwgbnVsbCwgZmFsc2UpOwogIH0gZmluYWxseSB7IHNldEJ1",
        "c3koZmFsc2UpOyB9Cn0KYXN5bmMgZnVuY3Rpb24gY2FuY2VsUSgpeyB0cnkgeyBhd2FpdCBmZXRjaCgnL2FwaS9jYW5jZWw/cWlk",
        "PScgKyBlbmNvZGVVUklDb21wb25lbnQoY3VyUWlkKSwge21ldGhvZDonUE9TVCd9KTsgfSBjYXRjaChlKXt9IH0KCi8qIC0tLS0g",
        "Z3JpZCAtLS0tICovCmNvbnN0IE5VTVJFID0gL0lOVHxERUNJTUFMfERPVUJMRXxGTE9BVHxSRUFMfE5VTUVSSUN8SFVHRUlOVHxV",
        "QklHSU5UL2k7CmNvbnN0IGlzTnVtID0gaSA9PiBOVU1SRS50ZXN0KGNvbHNbaV0udCkgJiYgIS9cW1xdfExJU1R8U1RSVUNUfE1B",
        "UC9pLnRlc3QoY29sc1tpXS50KTsKZnVuY3Rpb24gcmVuZGVyR3JpZCgpewogIGlmICghY29scy5sZW5ndGgpeyByZXR1cm47IH0K",
        "ICBjb25zdCBoID0gWyc8dGFibGU+PHRoZWFkPjx0cj48dGggY2xhc3M9ImlkeCI+IzwvdGg+J107CiAgY29scy5mb3JFYWNoKChj",
        "LCBpKSA9PiBoLnB1c2goJzx0aCBkYXRhLWk9IicgKyBpICsgJyIgdGl0bGU9IicgKyBlc2MoYy50KSArICciPicgKyBlc2MoYy5u",
        "KSArCiAgICAnPHNtYWxsPicgKyBlc2MoYy50KSArICc8L3NtYWxsPicgKyAoaSA9PT0gc29ydENvbCA/ICc8c3BhbiBjbGFzcz0i",
        "YXIiPiAnICsgKHNvcnREaXIgPiAwID8gJ+KWsicgOiAn4pa8JykgKyAnPC9zcGFuPicgOiAnJykgKyAnPC90aD4nKSk7CiAgaC5w",
        "dXNoKCc8L3RyPjwvdGhlYWQ+PHRib2R5PicpOwogIGNvbnN0IG51bXMgPSBjb2xzLm1hcCgoXywgaSkgPT4gaXNOdW0oaSkpOwog",
        "IHJvd3MuZm9yRWFjaCgociwgcmkpID0+IHsKICAgIGgucHVzaCgnPHRyPjx0ZCBjbGFzcz0iaWR4Ij4nICsgKHJpICsgMSkgKyAn",
        "PC90ZD4nKTsKICAgIGZvciAobGV0IGkgPSAwOyBpIDwgci5sZW5ndGg7IGkrKyl7CiAgICAgIGNvbnN0IHYgPSByW2ldOwogICAg",
        "ICBpZiAodiA9PT0gbnVsbCkgaC5wdXNoKCc8dGQgY2xhc3M9Im51bGwiPk5VTEw8L3RkPicpOwogICAgICBlbHNlIGgucHVzaCgn",
        "PHRkJyArIChudW1zW2ldID8gJyBjbGFzcz0ibnVtIicgOiAnJykgKyAnIHRpdGxlPSInICsgZXNjKHYpICsgJyI+JyArIGVzYyh2",
        "KSArICc8L3RkPicpOwogICAgfQogICAgaC5wdXNoKCc8L3RyPicpOwogIH0pOwogIGgucHVzaCgnPC90Ym9keT48L3RhYmxlPicp",
        "OwogIGlmICghcm93cy5sZW5ndGgpIGgucHVzaCgnPGRpdiBjbGFzcz0iZW1wdHkiPk5vIHJvd3MuPC9kaXY+Jyk7CiAgJCgnI2dy",
        "aWQnKS5pbm5lckhUTUwgPSBoLmpvaW4oJycpOwogICQoJyNncmlkJykucXVlcnlTZWxlY3RvckFsbCgndGhbZGF0YS1pXScpLmZv",
        "ckVhY2godGggPT4gdGgub25jbGljayA9ICgpID0+IHNvcnRCeSgrdGguZGF0YXNldC5pKSk7Cn0KZnVuY3Rpb24gc29ydEJ5KGkp",
        "ewogIHNvcnREaXIgPSAoc29ydENvbCA9PT0gaSkgPyAtc29ydERpciA6IDE7IHNvcnRDb2wgPSBpOwogIGNvbnN0IG51bSA9IGlz",
        "TnVtKGkpOwogIHJvd3Muc29ydCgoYSwgYikgPT4gewogICAgY29uc3QgeCA9IGFbaV0sIHkgPSBiW2ldOwogICAgaWYgKHggPT09",
        "IG51bGwgfHwgeSA9PT0gbnVsbCkgcmV0dXJuICh4ID09PSBudWxsID8gMSA6IDApIC0gKHkgPT09IG51bGwgPyAxIDogMCk7ICAg",
        "Ly8gTlVMTHMgbGFzdAogICAgY29uc3QgYyA9IG51bSA/IChwYXJzZUZsb2F0KHgpIC0gcGFyc2VGbG9hdCh5KSkgOiBTdHJpbmco",
        "eCkubG9jYWxlQ29tcGFyZShTdHJpbmcoeSkpOwogICAgcmV0dXJuIChjIHx8IDApICogc29ydERpcjsKICB9KTsKICByZW5kZXJH",
        "cmlkKCk7Cn0KCi8qIC0tLS0gZXhwb3J0IC0tLS0gKi8KYXN5bmMgZnVuY3Rpb24gZXhwb3J0Q3N2KCl7CiAgaWYgKCFsYXN0U3Fs",
        "KSByZXR1cm47CiAgJCgnI2V4cG9ydCcpLmRpc2FibGVkID0gdHJ1ZTsgc3RhdHVzKCdFeHBvcnRpbmcgYWxsIHJvd3PigKYnKTsK",
        "ICB0cnkgewogICAgY29uc3QgciA9IGF3YWl0IGZldGNoKCcvYXBpL2V4cG9ydCcsIHttZXRob2Q6J1BPU1QnLCBib2R5OiBsYXN0",
        "U3FsfSk7CiAgICBpZiAoIXIub2speyBsZXQgbSA9ICdIVFRQICcgKyByLnN0YXR1czsgdHJ5IHsgbSA9IChhd2FpdCByLmpzb24o",
        "KSkuZXJyb3I7IH0gY2F0Y2goZSl7fSB0aHJvdyBuZXcgRXJyb3IobSk7IH0KICAgIGNvbnN0IGJsb2IgPSBhd2FpdCByLmJsb2Io",
        "KTsKICAgIGNvbnN0IGEgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCdhJyk7CiAgICBhLmhyZWYgPSBVUkwuY3JlYXRlT2JqZWN0",
        "VVJMKGJsb2IpOwogICAgYS5kb3dubG9hZCA9ICdyZXN1bHQtJyArIG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKS5yZXBsYWNlKC9b",
        "Oi5dL2csICctJykuc2xpY2UoMCwgMTkpICsgJy5jc3YnOwogICAgZG9jdW1lbnQuYm9keS5hcHBlbmRDaGlsZChhKTsgYS5jbGlj",
        "aygpOyBhLnJlbW92ZSgpOwogICAgc2V0VGltZW91dCgoKSA9PiBVUkwucmV2b2tlT2JqZWN0VVJMKGEuaHJlZiksIDUwMDApOwog",
        "ICAgc3RhdHVzKCdFeHBvcnRlZCAnICsgKGJsb2Iuc2l6ZSAvIDEwMjQpLnRvRml4ZWQoMSkgKyAnIEtCLicsICdvaycpOwogIH0g",
        "Y2F0Y2goZSl7IHN0YXR1cyhlLm1lc3NhZ2UsICdlcnInKTsgfQogIGZpbmFsbHkgeyAkKCcjZXhwb3J0JykuZGlzYWJsZWQgPSBm",
        "YWxzZTsgfQp9CgovKiAtLS0tIGZpbGUgYnJvd3NlciAtLS0tICovCmNvbnN0IGZtdFNpemUgPSBuID0+IG4gPCAxMDI0ID8gbiAr",
        "ICcgQicgOiBuIDwgMTA0ODU3NiA/IChuLzEwMjQpLnRvRml4ZWQoMSkgKyAnIEtCJyA6IG4gPCAxMDczNzQxODI0ID8gKG4vMTA0",
        "ODU3NikudG9GaXhlZCgxKSArICcgTUInIDogKG4vMTA3Mzc0MTgyNCkudG9GaXhlZCgyKSArICcgR0InOwphc3luYyBmdW5jdGlv",
        "biBicm93c2UocCl7CiAgdHJ5IHsKICAgIGNvbnN0IGogPSBhd2FpdCBhcGkoJy9hcGkvZmlsZXMnICsgKHAgPyAnP3BhdGg9JyAr",
        "IGVuY29kZVVSSUNvbXBvbmVudChwKSA6ICcnKSk7CiAgICBjd2QgPSBqLnBhdGg7ICQoJyNwYXRoJykudmFsdWUgPSBjd2Q7ICQo",
        "JyN1cCcpLmRpc2FibGVkID0gIWoucGFyZW50OyAkKCcjdXAnKS5kYXRhc2V0LnAgPSBqLnBhcmVudCB8fCAnJzsKICAgIGNvbnN0",
        "IGVsID0gJCgnI2ZpbGVzJyk7CiAgICBpZiAoIWouZW50cmllcy5sZW5ndGgpeyBlbC5pbm5lckhUTUwgPSAnPGRpdiBjbGFzcz0i",
        "aGludCI+RW1wdHkgZm9sZGVyLjwvZGl2Pic7IHJldHVybjsgfQogICAgZWwuaW5uZXJIVE1MID0gai5lbnRyaWVzLm1hcCgoZSwg",
        "aSkgPT4KICAgICAgJzxkaXYgY2xhc3M9InJvdycgKyAoZS5kID8gJyBkaXInIDogJycpICsgJyIgZGF0YS1pPSInICsgaSArICci",
        "PjxzcGFuIGNsYXNzPSJuIiB0aXRsZT0iJyArIGVzYyhlLm4pICsgJyI+JyArCiAgICAgIChlLmQgPyAnJiMxMjgxOTM7ICcgOiAn",
        "JykgKyBlc2MoZS5uKSArICc8L3NwYW4+PHNwYW4gY2xhc3M9InMiPicgKyAoZS5kID8gJycgOiBmbXRTaXplKGUucykpICsgJzwv",
        "c3Bhbj48L2Rpdj4nKS5qb2luKCcnKTsKICAgIGVsLnF1ZXJ5U2VsZWN0b3JBbGwoJy5yb3cnKS5mb3JFYWNoKHIgPT4gci5vbmNs",
        "aWNrID0gKCkgPT4gewogICAgICBjb25zdCBlID0gai5lbnRyaWVzWytyLmRhdGFzZXQuaV07CiAgICAgIGNvbnN0IGZ1bGwgPSAo",
        "Y3dkLmVuZHNXaXRoKCcvJykgPyBjd2QgOiBjd2QgKyAnLycpICsgZS5uOwogICAgICBpZiAoZS5kKSBicm93c2UoZnVsbCk7IGVs",
        "c2UgaW5zZXJ0U25pcHBldChzbmlwcGV0Rm9yKGZ1bGwpKTsKICAgIH0pOwogIH0gY2F0Y2goZSl7ICQoJyNmaWxlcycpLmlubmVy",
        "SFRNTCA9ICc8ZGl2IGNsYXNzPSJoaW50IiBzdHlsZT0iY29sb3I6dmFyKC0tZXJyKSI+JyArIGVzYyhlLm1lc3NhZ2UpICsgJzwv",
        "ZGl2Pic7IH0KfQpjb25zdCBxID0gcyA9PiAiJyIgKyBzLnJlcGxhY2UoLycvZywgIicnIikgKyAiJyI7CmZ1bmN0aW9uIHNuaXBw",
        "ZXRGb3IocCl7CiAgY29uc3QgbCA9IHAudG9Mb3dlckNhc2UoKTsKICBpZiAoL1wucGFycXVldCQvLnRlc3QobCkpIHJldHVybiAn",
        "U0VMRUNUICogRlJPTSByZWFkX3BhcnF1ZXQoJyArIHEocCkgKyAnKSBMSU1JVCAxMDA7JzsKICBpZiAoL1wuKGNzdnx0c3Z8dHh0",
        "KShcLmd6KT8kLy50ZXN0KGwpKSByZXR1cm4gJ1NFTEVDVCAqIEZST00gcmVhZF9jc3YoJyArIHEocCkgKyAnKSBMSU1JVCAxMDA7",
        "JzsKICBpZiAoL1wuKGpzb258anNvbmx8bmRqc29uKShcLmd6KT8kLy50ZXN0KGwpKSByZXR1cm4gJ1NFTEVDVCAqIEZST00gcmVh",
        "ZF9qc29uX2F1dG8oJyArIHEocCkgKyAnKSBMSU1JVCAxMDA7JzsKICBpZiAoL1wuKGR1Y2tkYnxkYikkLy50ZXN0KGwpKSByZXR1",
        "cm4gJ0FUVEFDSCAnICsgcShwKSArICcgQVMgZGIxIChSRUFEX09OTFkpOyc7CiAgcmV0dXJuICdTRUxFQ1QgKiBGUk9NICcgKyBx",
        "KHApICsgJyBMSU1JVCAxMDA7JzsKfQpmdW5jdGlvbiBpbnNlcnRTbmlwcGV0KHQpewogIGNvbnN0IHYgPSBlZC52YWx1ZSwgcyA9",
        "IGVkLnNlbGVjdGlvblN0YXJ0LCBlID0gZWQuc2VsZWN0aW9uRW5kOwogIGNvbnN0IHByZSA9IHYuc2xpY2UoMCwgcyksIG5lZWRO",
        "bCA9IHByZS5sZW5ndGggJiYgIXByZS5lbmRzV2l0aCgnXG4nKTsKICBjb25zdCBpbnMgPSAobmVlZE5sID8gJ1xuJyA6ICcnKSAr",
        "IHQgKyAnXG4nOwogIGVkLnZhbHVlID0gcHJlICsgaW5zICsgdi5zbGljZShlKTsKICBlZC5mb2N1cygpOyBlZC5zZWxlY3Rpb25T",
        "dGFydCA9IHMgKyAobmVlZE5sID8gMSA6IDApOyBlZC5zZWxlY3Rpb25FbmQgPSBzICsgaW5zLmxlbmd0aCAtIDE7Cn0KJCgnI2dv",
        "Jykub25jbGljayA9ICgpID0+IGJyb3dzZSgkKCcjcGF0aCcpLnZhbHVlLnRyaW0oKSk7CiQoJyNwYXRoJykub25rZXlkb3duID0g",
        "ZSA9PiB7IGlmIChlLmtleSA9PT0gJ0VudGVyJykgYnJvd3NlKCQoJyNwYXRoJykudmFsdWUudHJpbSgpKTsgfTsKJCgnI3VwJyku",
        "b25jbGljayA9ICgpID0+IGJyb3dzZSgkKCcjdXAnKS5kYXRhc2V0LnApOwoKLyogLS0tLSBoaXN0b3J5ICsgc2F2ZWQgLS0tLSAq",
        "LwpmdW5jdGlvbiBhZGRIaXN0b3J5KHNxbCwgbXMsIG4sIG9rKXsKICBjb25zdCBoID0gc3RvcmUuZ2V0KCdkdWNrLmhpc3Rvcnkn",
        "LCBbXSkuZmlsdGVyKHggPT4geC5zcWwgIT09IHNxbCk7CiAgaC51bnNoaWZ0KHtzcWwsIG1zLCBuLCBvaywgdHM6IERhdGUubm93",
        "KCl9KTsKICBzdG9yZS5zZXQoJ2R1Y2suaGlzdG9yeScsIGguc2xpY2UoMCwgNjApKTsgcmVuZGVySGlzdG9yeSgpOwp9CmZ1bmN0",
        "aW9uIGl0ZW1IdG1sKHgsIGksIGtpbmQsIHJpZ2h0KXsKICByZXR1cm4gJzxkaXYgY2xhc3M9Iml0ZW0iIGRhdGEtaT0iJyArIGkg",
        "KyAnIiBkYXRhLWs9IicgKyBraW5kICsgJyI+PHByZT4nICsgZXNjKHguc3FsKSArICc8L3ByZT48c21hbGw+PHNwYW4+JyArIHJp",
        "Z2h0ICsKICAgICc8L3NwYW4+PGJ1dHRvbiBjbGFzcz0ieCIgZGF0YS1kZWw9IicgKyBpICsgJyIgdGl0bGU9IkRlbGV0ZSI+JnRp",
        "bWVzOzwvYnV0dG9uPjwvc21hbGw+PC9kaXY+JzsKfQpmdW5jdGlvbiB3aXJlSXRlbXMoZWwsIGtpbmQsIGFyciwga2V5KXsKICBl",
        "bC5xdWVyeVNlbGVjdG9yQWxsKCcuaXRlbScpLmZvckVhY2goaXQgPT4gewogICAgaXQub25jbGljayA9IGV2ID0+IHsKICAgICAg",
        "Y29uc3QgaSA9ICtpdC5kYXRhc2V0Lmk7CiAgICAgIGlmIChldi50YXJnZXQuZGF0YXNldC5kZWwgIT09IHVuZGVmaW5lZCl7CiAg",
        "ICAgICAgYXJyLnNwbGljZShpLCAxKTsgc3RvcmUuc2V0KGtleSwgYXJyKTsgKGtpbmQgPT09ICdoJyA/IHJlbmRlckhpc3Rvcnkg",
        "OiByZW5kZXJTYXZlZCkoKTsgcmV0dXJuOwogICAgICB9CiAgICAgIGVkLnZhbHVlID0gYXJyW2ldLnNxbDsgZWQuZm9jdXMoKTsK",
        "ICAgIH07CiAgfSk7Cn0KZnVuY3Rpb24gcmVuZGVySGlzdG9yeSgpewogIGNvbnN0IGggPSBzdG9yZS5nZXQoJ2R1Y2suaGlzdG9y",
        "eScsIFtdKSwgZWwgPSAkKCcjaGlzdG9yeScpOwogIGVsLmlubmVySFRNTCA9IGgubGVuZ3RoID8gaC5tYXAoKHgsIGkpID0+IGl0",
        "ZW1IdG1sKHgsIGksICdoJywKICAgICh4Lm9rID8gKHgubiA9PT0gbnVsbCA/ICdvaycgOiB4Lm4gKyAnIHJvd3MnKSArICh4Lm1z",
        "ICE9IG51bGwgPyAnIMK3ICcgKyB4Lm1zICsgJyBtcycgOiAnJykgOiAnZmFpbGVkJykgKyAnIMK3ICcgKyBuZXcgRGF0ZSh4LnRz",
        "KS50b0xvY2FsZVN0cmluZygpKSkuam9pbignJykKICAgIDogJzxkaXYgY2xhc3M9ImhpbnQiPk5vdGhpbmcgcnVuIHlldC4gSGlz",
        "dG9yeSBzdGF5cyBpbiB0aGlzIGJyb3dzZXIgb25seS48L2Rpdj4nOwogIHdpcmVJdGVtcyhlbCwgJ2gnLCBoLCAnZHVjay5oaXN0",
        "b3J5Jyk7Cn0KZnVuY3Rpb24gcmVuZGVyU2F2ZWQoKXsKICBjb25zdCBzID0gc3RvcmUuZ2V0KCdkdWNrLnNhdmVkJywgW10pLCBl",
        "bCA9ICQoJyNzYXZlZCcpOwogIGVsLmlubmVySFRNTCA9IHMubGVuZ3RoID8gcy5tYXAoKHgsIGkpID0+IGl0ZW1IdG1sKHgsIGks",
        "ICdzJywgJzxiPicgKyBlc2MoeC5uYW1lKSArICc8L2I+JykpLmpvaW4oJycpCiAgICA6ICc8ZGl2IGNsYXNzPSJoaW50Ij5Vc2Ug",
        "4oCcU2F2ZSBxdWVyeeKAnSB0byBrZWVwIGEgcXVlcnkgaGVyZSAoc3RvcmVkIGluIHRoaXMgYnJvd3NlciBvbmx5KS48L2Rpdj4n",
        "OwogIHdpcmVJdGVtcyhlbCwgJ3MnLCBzLCAnZHVjay5zYXZlZCcpOwp9CiQoJyNzYXZlJykub25jbGljayA9ICgpID0+IHsKICBj",
        "b25zdCBzcWwgPSBlZC52YWx1ZS50cmltKCk7IGlmICghc3FsKSByZXR1cm47CiAgY29uc3QgbmFtZSA9IHByb21wdCgnTmFtZSBm",
        "b3IgdGhpcyBxdWVyeTonLCBzcWwuc3BsaXQoJ1xuJylbMF0uc2xpY2UoMCwgNDApKTsKICBpZiAoIW5hbWUpIHJldHVybjsKICBj",
        "b25zdCBzID0gc3RvcmUuZ2V0KCdkdWNrLnNhdmVkJywgW10pOyBzLnVuc2hpZnQoe25hbWUsIHNxbH0pOyBzdG9yZS5zZXQoJ2R1",
        "Y2suc2F2ZWQnLCBzKTsgcmVuZGVyU2F2ZWQoKTsKfTsKCi8qIC0tLS0gd2lyaW5nIC0tLS0gKi8KJCgnI3J1bicpLm9uY2xpY2sg",
        "PSBydW47ICQoJyNjYW5jZWwnKS5vbmNsaWNrID0gY2FuY2VsUTsgJCgnI2V4cG9ydCcpLm9uY2xpY2sgPSBleHBvcnRDc3Y7CmVk",
        "Lm9ua2V5ZG93biA9IGUgPT4gewogIGlmICgoZS5jdHJsS2V5IHx8IGUubWV0YUtleSkgJiYgZS5rZXkgPT09ICdFbnRlcicpeyBl",
        "LnByZXZlbnREZWZhdWx0KCk7IHJ1bigpOyB9CiAgZWxzZSBpZiAoZS5rZXkgPT09ICdUYWInICYmICFlLnNoaWZ0S2V5KXsKICAg",
        "IGUucHJldmVudERlZmF1bHQoKTsgY29uc3QgcyA9IGVkLnNlbGVjdGlvblN0YXJ0OwogICAgZWQudmFsdWUgPSBlZC52YWx1ZS5z",
        "bGljZSgwLCBzKSArICcgICcgKyBlZC52YWx1ZS5zbGljZShlZC5zZWxlY3Rpb25FbmQpOwogICAgZWQuc2VsZWN0aW9uU3RhcnQg",
        "PSBlZC5zZWxlY3Rpb25FbmQgPSBzICsgMjsKICB9Cn07CmVkLm9uaW5wdXQgPSAoKSA9PiBzdG9yZS5zZXQoJ2R1Y2suZHJhZnQn",
        "LCBlZC52YWx1ZSk7CmVkLnZhbHVlID0gc3RvcmUuZ2V0KCdkdWNrLmRyYWZ0JywgJycpOwoKKGFzeW5jICgpID0+IHsKICB0cnkg",
        "ewogICAgY29uc3QgaSA9IGF3YWl0IGFwaSgnL2FwaS9pbmZvJyk7CiAgICAkKCcjbWV0YScpLnRleHRDb250ZW50ID0gJ0R1Y2tE",
        "QiAnICsgaS52ZXJzaW9uICsgJyDCtyBkYXRhYmFzZTogJyArIGkuZGIgKyAoaS5yZWFkb25seSA/ICcgKHJlYWQtb25seSknIDog",
        "JycpICsgJyDCtyBKYXZhICcgKyBpLmphdmE7CiAgICBicm93c2UoaS5zdGFydCk7CiAgfSBjYXRjaChlKXsgJCgnI21ldGEnKS50",
        "ZXh0Q29udGVudCA9ICdlcnJvcjogJyArIGUubWVzc2FnZTsgfQogIHJlbmRlckhpc3RvcnkoKTsgcmVuZGVyU2F2ZWQoKTsKfSko",
        "KTsKPC9zY3JpcHQ+CjwvYm9keT4KPC9odG1sPgo="
    };
}
