// One disposable, account-free libSQL server fixture for the dalgo2libsql
// integration suite and manual repro. The image is commit-pinned by digest
// (never `latest`), the Ed25519 key pair and JWTs are generated per run, and
// teardown removes only this container, its temporary key directory and its own
// recorded data volumes.
//
// Usage:
//
//   node scripts/libsql-fixture.mjs                       # start, seed, print env, keep running
//   node scripts/libsql-fixture.mjs -- keep              # same
//   node scripts/libsql-fixture.mjs -- go test ./... -run TestIntegration -v
//   node scripts/libsql-fixture.mjs --check-pins
//
// The child command runs with LIBSQL_FIXTURE_URL, LIBSQL_RO_TOKEN and
// LIBSQL_REQUIRE_FIXTURE=1. The writer token stays in this script, which uses
// it only to seed; the test process cannot use it. The child's exit status is
// the script's; a teardown failure always fails the run.
import { execFileSync, spawn, spawnSync } from "node:child_process";
import { generateKeyPairSync, randomBytes, sign } from "node:crypto";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const LIBSQL_IMAGE =
  "ghcr.io/tursodatabase/libsql-server@sha256:6dd3eb276d9d3604e4a48ac4a999a2e267814732d57d7e94c04ba71482333a67";
export const LIBSQL_SOURCE_COMMIT = "d6c75af6353bb1c34985399608e37cd272a35aa1";
export const LIBSQL_ARM64_MANIFEST = "sha256:f5fef78e7e927dcad04da5a95cd07b589c1bc6ef174ece71f02582cfbcbbe2a0";

const TOKEN_TTL_SECONDS = 3_600;

function base64url(value) {
  return Buffer.from(value).toString("base64url");
}

function log(message) {
  process.stderr.write(`[libsql-fixture] ${message}\n`);
}

/** Verifies the pinned multi-arch image metadata before starting anything. */
export function verifyPinnedImage({ run = execFileSync } = {}) {
  if (LIBSQL_IMAGE.includes(":latest")) {
    throw new Error("the libSQL fixture must not use a latest tag");
  }
  const raw = run("docker", ["manifest", "inspect", LIBSQL_IMAGE], { encoding: "utf8" });
  const index = JSON.parse(raw);
  if (index.schemaVersion !== 2 || !Array.isArray(index.manifests)) {
    throw new Error("unexpected libSQL image index");
  }
  const arm = index.manifests.find((entry) => entry.platform?.architecture === "arm64" && entry.platform?.os === "linux");
  const amd = index.manifests.find((entry) => entry.platform?.architecture === "amd64" && entry.platform?.os === "linux");
  if (arm?.digest !== LIBSQL_ARM64_MANIFEST) {
    throw new Error(`unexpected ARM64 manifest ${String(arm?.digest)}`);
  }
  if (amd === undefined) {
    throw new Error("the libSQL image has no amd64 manifest");
  }
  return {
    image: LIBSQL_IMAGE,
    sourceCommit: LIBSQL_SOURCE_COMMIT,
    arm64: arm.digest,
    amd64: amd.digest,
  };
}

function pipelineBody(sql, args = [], wantRows = false) {
  return JSON.stringify({
    baton: null,
    requests: [
      { type: "execute", stmt: { sql, args, want_rows: wantRows } },
      { type: "close" },
    ],
  });
}

function isMissingDockerObject(error) {
  return typeof error === "object" && error !== null
    && error.status === 1
    && /no such (container|object|volume)/iu.test(String(error.stderr ?? ""));
}

function dockerRun(run, args, options = {}) {
  return run("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], ...options });
}

/**
 * Best-effort container state and tail of its logs for a failed health check.
 * Diagnostics never throw and never hide the original health failure.
 */
function containerDiagnostics(name) {
  const inspect = spawnSync("docker", ["inspect", "--format", "{{json .State}}", name], { encoding: "utf8" });
  const logs = spawnSync("docker", ["logs", "--tail", "80", name], { encoding: "utf8" });
  const state = (inspect.stdout || inspect.stderr || "").trim();
  const output = [logs.stdout, logs.stderr].filter(Boolean).join("").trim();
  return `state: ${state || "unavailable"}\nlogs:\n${output || "unavailable"}`;
}

/**
 * Captures the exact volume mounts this container owns. Anonymous volumes
 * declared by the image arrive as 64-hex names attached to our own container;
 * only those are ever removed. The mounts are read while the container still
 * exists, before `docker rm`, so ownership is recorded rather than guessed.
 */
function inspectOwnedVolumes(run, name) {
  let output;
  try {
    output = dockerRun(run, ["inspect", "--format", "{{json .Mounts}}", name]);
  } catch (error) {
    if (isMissingDockerObject(error)) return null;
    throw error;
  }
  const mounts = JSON.parse(output);
  if (mounts === null) return [];
  if (!Array.isArray(mounts)) throw new Error("unexpected docker inspect mounts for the libSQL fixture");
  return mounts
    .filter((mount) => mount?.Type === "volume" && typeof mount.Name === "string")
    .map((mount) => mount.Name);
}

function volumeExists(run, volume) {
  try {
    dockerRun(run, ["volume", "inspect", volume]);
    return true;
  } catch (error) {
    if (isMissingDockerObject(error)) return false;
    throw error;
  }
}

/**
 * Removes only this fixture's own container and the exact volumes recorded from
 * it, then verifies the real Docker state. An already-absent container is
 * success; a real Docker failure is never swallowed.
 */
export function teardownFixtureResources({ name, run = execFileSync }) {
  const ownedVolumes = inspectOwnedVolumes(run, name);
  if (ownedVolumes !== null) {
    try {
      dockerRun(run, ["rm", "-f", "-v", name]);
    } catch (error) {
      if (!isMissingDockerObject(error)) throw error;
    }
  }
  if (inspectOwnedVolumes(run, name) !== null) {
    throw new Error(`libSQL fixture container still exists after teardown: ${name}`);
  }

  const removed = [];
  const remaining = [];
  for (const volume of ownedVolumes ?? []) {
    if (volumeExists(run, volume)) {
      try {
        dockerRun(run, ["volume", "rm", "-f", volume]);
      } catch (error) {
        if (!isMissingDockerObject(error)) throw error;
      }
      removed.push(volume);
    }
    if (volumeExists(run, volume)) remaining.push(volume);
  }
  if (remaining.length > 0) {
    throw new Error(`libSQL fixture volumes still exist after teardown: ${remaining.join(", ")}`);
  }
  return {
    containerRemoved: ownedVolumes !== null,
    volumesVerified: ownedVolumes ?? [],
    volumesRemoved: removed,
  };
}

export async function execute(url, token, sql, args = [], wantRows = false) {
  const response = await fetch(`${url}/v3/pipeline`, {
    method: "POST",
    redirect: "error",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
      accept: "application/json",
    },
    body: pipelineBody(sql, args, wantRows),
  });
  if (!response.ok) {
    throw new Error(`fixture statement failed with HTTP ${String(response.status)}`);
  }
  const value = await response.json();
  const first = value?.results?.[0];
  if (first?.type !== "ok") {
    throw new Error(`fixture statement failed: ${String(first?.error?.code ?? "unknown")}`);
  }
  return first.response.result;
}

/** Asserts that the reader token cannot write: the server must answer 403. */
export async function assertReaderCannotWrite(url, readerToken) {
  const response = await fetch(`${url}/v3/pipeline`, {
    method: "POST",
    redirect: "error",
    headers: {
      authorization: `Bearer ${readerToken}`,
      "content-type": "application/json",
      accept: "application/json",
    },
    body: pipelineBody("CREATE TABLE reader_write_probe (id INTEGER)"),
  });
  if (response.status !== 403) {
    throw new Error(`the reader token was expected to be refused with 403, got HTTP ${String(response.status)}`);
  }
}

/** Seeds native tables with duplicate sort values, NULLs, fractions, blobs and big integers. */
export async function seedLibSQL(url, writerToken) {
  const statements = [
    "CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT NOT NULL, rank INTEGER, score REAL, done INTEGER, code TEXT, payload BLOB)",
    `INSERT INTO items (id, title, rank, score, done, code, payload) VALUES
      (1, 'Alpha', 2, 9.5, 0, 'a', x'00ff10'),
      (2, 'Beta', 2, NULL, 0, 'b', NULL),
      (3, 'Gamma', 1, 3.5, 0, 'c', x''),
      (4, 'Delta', NULL, NULL, 0, 'd', NULL),
      (5, 'Epsilon', 3, 4.25, 0, 'e', x'01'),
      (6, 'Zeta', 1, 7, 1, 'f', NULL),
      (7, 'Eta', 2, NULL, 1, 'g', NULL),
      (8, 'Theta', NULL, 0.5, 0, 'h', NULL),
      (9, 'Iota', 3, 1.25, 0, 'i', NULL),
      (10, 'Kappa', 1, NULL, 0, 'j', NULL),
      (11, 'Lambda', 2, 2.5, 1, 'k', NULL),
      (12, 'Mu', 3, NULL, 0, 'l', NULL)`,
    "CREATE TABLE labels (code TEXT PRIMARY KEY, label TEXT)",
    "INSERT INTO labels (code, label) VALUES ('a1', 'Alpha'), ('b2', 'Beta'), ('c3', 'Gamma')",
    "CREATE UNIQUE INDEX labels_label_u ON labels(label)",
    "CREATE TABLE metrics (ratio REAL PRIMARY KEY, label TEXT)",
    "INSERT INTO metrics (ratio, label) VALUES (0.1, 'tenth'), (0.2, 'fifth'), (0.30000000000000004, 'sum'), (1.5, 'half')",
    "CREATE TABLE composite (a TEXT NOT NULL, b TEXT NOT NULL, value TEXT, PRIMARY KEY (a, b))",
    "INSERT INTO composite (a, b, value) VALUES ('x', '1', 'first'), ('x', '2', 'second')",
    "CREATE TABLE nullable_pk (code TEXT PRIMARY KEY, label TEXT)",
    "INSERT INTO nullable_pk (code, label) VALUES ('n1', 'explicit'), (NULL, 'null-code')",
    "CREATE TABLE indexed_only (slug TEXT NOT NULL, label TEXT)",
    "CREATE UNIQUE INDEX indexed_only_slug_u ON indexed_only(slug)",
    "INSERT INTO indexed_only (slug, label) VALUES ('alpha', 'one'), ('beta', 'two'), ('gamma', 'three')",
    "CREATE TABLE pk_desc (id INTEGER PRIMARY KEY DESC, label TEXT)",
    "INSERT INTO pk_desc (id, label) VALUES (NULL, 'null-first'), (NULL, 'null-second'), (1, 'one'), (2, 'two')",
    "CREATE TABLE wr_items (code TEXT NOT NULL PRIMARY KEY, label TEXT) WITHOUT ROWID",
    "INSERT INTO wr_items (code, label) VALUES ('wa', 'A'), ('wb', 'B')",
    "CREATE TABLE blobs (id INTEGER PRIMARY KEY, payload BLOB)",
    "INSERT INTO blobs (id, payload) VALUES (1, x'00ff10'), (2, NULL), (3, x'')",
    "CREATE TABLE bigints (id INTEGER PRIMARY KEY, label TEXT)",
    `INSERT INTO bigints (id, label) VALUES
      (9223372036854775807, 'max'),
      (-9223372036854775808, 'min'),
      (9007199254740993, 'above-2^53')`,
    // SQLite's dynamic typing: the declared affinity does not change a stored
    // value. declared_integer holds every storage class in different rows, and
    // declared_text holds text, a blob, an integer and NULL.
    "CREATE TABLE mixed (id INTEGER PRIMARY KEY, declared_integer INTEGER, declared_text TEXT)",
    `INSERT INTO mixed (id, declared_integer, declared_text) VALUES
      (1, 'text-in-integer', x'000102'),
      (2, 42, 'plain'),
      (3, 2.5, NULL),
      (4, x'0aff', 'seven'),
      (5, NULL, 'last')`,
    // A real foreign key, so the schema reader's foreign-key metadata and its
    // read-only enforcement query are exercised instead of an unconstrained
    // schema that would never run them.
    "CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
    "INSERT INTO fk_parent (id, label) VALUES (1, 'parent-one'), (2, 'parent-two')",
    `CREATE TABLE fk_child (
      id INTEGER PRIMARY KEY,
      parent_id INTEGER NOT NULL,
      label TEXT,
      FOREIGN KEY (parent_id) REFERENCES fk_parent(id) ON UPDATE CASCADE ON DELETE RESTRICT
    )`,
    "INSERT INTO fk_child (id, parent_id, label) VALUES (1, 1, 'child-one'), (2, 2, 'child-two')",
    "CREATE VIEW items_view AS SELECT id, title, rank FROM items",
  ];
  for (const sql of statements) {
    await execute(url, writerToken, sql);
  }
}

/**
 * Starts one unique loopback container and returns its origin, reader/writer
 * tokens and an idempotent stop callback. Docker publishes a random loopback
 * host port; nothing is exposed beyond 127.0.0.1.
 */
export async function startLibSQLFixture() {
  const metadata = verifyPinnedImage();
  const stamp = `${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
  const name = `dalgo-libsql-fixture-${stamp}`;
  const work = mkdtempSync(join(tmpdir(), `${name}-`));
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const publicKeyFile = join(work, "public.pem");
  writeFileSync(publicKeyFile, publicKey.export({ type: "spki", format: "pem" }));
  // mkdtemp creates the directory 0700. On a Linux runner the container runs
  // as its own user, so a 0700 host directory and a 0600 key file are not
  // traversable/readable through a bind mount (the server exits and the health
  // check sees only a refused fetch). The public key is not secret: make the
  // directory and key world-readable, never the private key, which stays in
  // this process only.
  chmodSync(work, 0o755);
  chmodSync(publicKeyFile, 0o644);

  const token = (permission) => {
    const header = base64url(JSON.stringify({ alg: "EdDSA", typ: "JWT" }));
    const payload = base64url(JSON.stringify({
      a: permission,
      exp: Math.floor(Date.now() / 1000) + TOKEN_TTL_SECONDS,
    }));
    return `${header}.${payload}.${sign(null, Buffer.from(`${header}.${payload}`), privateKey).toString("base64url")}`;
  };
  const readerToken = token("ro");
  const writerToken = token("rw");

  let stopped = false;
  const stop = () => {
    if (stopped) return undefined;
    const result = teardownFixtureResources({ name });
    stopped = true;
    return result;
  };
  const onSignal = () => {
    try {
      stop();
    } catch (error) {
      process.stderr.write(`fixture teardown failed: ${error instanceof Error ? error.message : String(error)}\n`);
    }
    process.exit(130);
  };
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);

  try {
    execFileSync(
      "docker",
      [
        "run", "-d", "--name", name,
        "-p", "127.0.0.1::8080",
        "-v", `${work}:/keys:ro`,
        "-e", "SQLD_DB_PATH=/data/fixture",
        "-e", "SQLD_HTTP_LISTEN_ADDR=0.0.0.0:8080",
        "-e", "SQLD_AUTH_JWT_KEY_FILE=/keys/public.pem",
        LIBSQL_IMAGE,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    const portOutput = execFileSync("docker", ["port", name, "8080"], { encoding: "utf8" }).trim();
    const match = /:(\d+)$/u.exec(portOutput);
    if (match === null || match[1] === undefined) {
      throw new Error(`could not read the fixture port from: ${portOutput}`);
    }
    const url = `http://127.0.0.1:${match[1]}`;
    const deadline = Date.now() + 60_000;
    let last = "no response";
    let ready = false;
    while (Date.now() < deadline) {
      try {
        const response = await fetch(`${url}/health`, { redirect: "error" });
        if (response.ok) {
          ready = true;
          break;
        }
        last = `HTTP ${String(response.status)}`;
      } catch (error) {
        last = error instanceof Error ? error.message : String(error);
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    if (!ready) {
      throw new Error(`libSQL fixture did not become healthy: ${last}\n${containerDiagnostics(name)}`);
    }
    return {
      ...metadata,
      name,
      url,
      work,
      readerToken,
      writerToken,
      stop,
      discardWork() {
        rmSync(work, { recursive: true, force: true });
      },
      detachSignalHandlers() {
        process.removeListener("SIGINT", onSignal);
        process.removeListener("SIGTERM", onSignal);
      },
    };
  } catch (error) {
    try {
      stop();
    } catch (cleanupError) {
      process.stderr.write(`fixture teardown failed: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}\n`);
    }
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
    throw error;
  }
}

function runChild(command, argv, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, argv, { stdio: "inherit", env });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
}

async function main() {
  const args = process.argv.slice(2);
  if (args[0] === "--check-pins") {
    const metadata = verifyPinnedImage();
    process.stdout.write(`${JSON.stringify(metadata, null, 2)}\n`);
    return 0;
  }
  const separator = args.indexOf("--");
  const command = separator === -1 ? [] : args.slice(separator + 1);

  const fixture = await startLibSQLFixture();
  log(`container ${fixture.name} at ${fixture.url} (image ${fixture.image})`);
  let exitCode = 0;
  try {
    await assertReaderCannotWrite(fixture.url, fixture.readerToken);
    log("reader token refused a write with 403");
    await seedLibSQL(fixture.url, fixture.writerToken);
    log("native fixtures seeded");

    const env = {
      ...process.env,
      LIBSQL_FIXTURE_URL: fixture.url,
      LIBSQL_RO_TOKEN: fixture.readerToken,
      LIBSQL_REQUIRE_FIXTURE: "1",
    };
    if (command.length > 0) {
      log(`running: ${command.join(" ")}`);
      const result = await runChild(command[0], command.slice(1), env);
      if (result.signal) {
        exitCode = 1;
        log(`child terminated by ${result.signal}`);
      } else {
        exitCode = result.code ?? 1;
      }
    } else {
      process.stdout.write(`${JSON.stringify({
        url: fixture.url,
        readerToken: fixture.readerToken,
        writerToken: fixture.writerToken,
        container: fixture.name,
      }, null, 2)}\n`);
      log("no command given; the fixture stays up until this process is stopped (Ctrl-C)");
      await new Promise((resolve) => process.once("SIGINT", resolve));
    }
  } finally {
    try {
      const teardown = fixture.stop();
      log(`teardown: containerRemoved=${String(teardown.containerRemoved)} volumesVerified=${JSON.stringify(teardown.volumesVerified)} volumesRemoved=${JSON.stringify(teardown.volumesRemoved)}`);
      fixture.discardWork();
    } catch (error) {
      exitCode = 1;
      log(`teardown failed: ${error instanceof Error ? error.message : String(error)}`);
    }
    fixture.detachSignalHandlers();
  }
  return exitCode;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().then((code) => process.exit(code), (error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
}
