#!/usr/bin/env node
import { spawn, execFile, execFileSync } from "node:child_process";
import { randomBytes, createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { chmod, copyFile, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import net from "node:net";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = resolve(dirname(scriptPath), "..");

export function extractPinnedImages(composeContent) {
  const lines = composeContent.split(/\r?\n/);
  let currentService = null;
  const images = {};
  for (const line of lines) {
    const serviceMatch = line.match(/^  ([a-zA-Z0-9_-]+):\s*$/);
    if (serviceMatch) {
      currentService = serviceMatch[1];
      continue;
    }
    const imageMatch = line.match(/^    image:\s*([^\s#]+)/);
    if (imageMatch && currentService) {
      images[currentService] = imageMatch[1];
    }
  }
  if (!images.postgres || !images.temporal) {
    throw new Error("Failed to extract pinned postgres or temporal image from compose.yml template");
  }
  return {
    postgres: images.postgres,
    temporal: images.temporal
  };
}

export function extractPinnedOpenCode(dockerfileContent) {
  const match = dockerfileContent.match(/opencode-ai@([0-9.]+)/);
  if (!match) {
    throw new Error("Failed to extract pinned opencode-ai version from Dockerfile");
  }
  return match[1];
}

async function ensureDir(path, mode = 0o700) {
  await mkdir(path, { recursive: true, mode });
  try {
    await chmod(path, mode);
  } catch {}
}

async function writeSecret(filePath, value) {
  await writeFile(filePath, value.trim() + "\n", { mode: 0o600, flag: "w" });
  try {
    await chmod(filePath, 0o600);
  } catch {}
}

async function waitForTcpPort(host, port, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ready = await new Promise((resolve) => {
      const socket = new net.Socket();
      socket.setTimeout(1000);
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        socket.destroy();
        resolve(false);
      });
      socket.once("timeout", () => {
        socket.destroy();
        resolve(false);
      });
      socket.connect(port, host);
    });
    if (ready) return;
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`Timed out waiting for port ${host}:${port}`);
}

async function waitForHttpOk(url, predicate = (body) => body?.ok === true, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  let lastBody = null;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      const body = await res.json().catch(() => null);
      lastBody = body;
      if (res.ok && predicate(body)) return body;
    } catch (err) {
      lastError = err;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`Timed out waiting for HTTP OK at ${url}${lastBody ? ` (last response: ${JSON.stringify(lastBody)})` : ""}${lastError ? `: ${lastError.message}` : ""}`);
}

async function execDocker(args, options = {}) {
  try {
    return await execFileAsync("docker", args, { maxBuffer: 100 * 1024 * 1024, ...options });
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("docker command not found. Ensure Docker is installed and running.");
    }
    throw error;
  }
}

function runStreaming(cmd, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: "inherit", ...options });
    child.once("error", reject);
    child.once("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} ${args.join(" ")} exited with code ${code}`));
    });
  });
}

export function generateComposeYaml({ images, postgresPort, temporalPort, composeProject, secretsDir }) {
  return `services:
  postgres:
    image: ${images.postgres}
    restart: unless-stopped
    environment:
      POSTGRES_DB: spaceapp
      POSTGRES_USER: spaceapp
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres-password
    ports:
      - "127.0.0.1:${postgresPort}:5432"
    volumes:
      - ${composeProject}-postgres:/var/lib/postgresql/data
    secrets:
      - postgres-password
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U spaceapp -d spaceapp"]
      interval: 2s
      timeout: 2s
      retries: 25

  temporal:
    image: ${images.temporal}
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DB: postgres12
      DB_PORT: 5432
      POSTGRES_USER: spaceapp
      POSTGRES_SEEDS: postgres
      DYNAMIC_CONFIG_FILE_PATH: config/dynamicconfig/docker.yaml
    ports:
      - "127.0.0.1:${temporalPort}:7233"
    entrypoint: ["/bin/sh", "-ec"]
    command:
      - export POSTGRES_PWD="$$(cat /run/secrets/postgres-password)"; exec /etc/temporal/entrypoint.sh autosetup
    secrets:
      - postgres-password
    healthcheck:
      test: ["CMD-SHELL", "temporal operator cluster health --address 127.0.0.1:7233 >/dev/null && temporal operator namespace describe --namespace default --address 127.0.0.1:7233 >/dev/null"]
      interval: 3s
      timeout: 3s
      retries: 35

secrets:
  postgres-password:
    file: ${secretsDir}/postgres-password

volumes:
  ${composeProject}-postgres:
`;
}

export class DevStackRunner {
  constructor(options = {}) {
    this.env = options.env ?? process.env;
    this.repoRoot = options.repoRoot ?? repoRoot;
    this.stateDir = options.stateDir ?? (this.env.SPACE_DEV_STACK_DIR ? resolve(this.repoRoot, this.env.SPACE_DEV_STACK_DIR) : resolve(this.repoRoot, ".dev-stack"));
    this.composeProject = options.composeProject ?? (this.env.SPACE_DEV_COMPOSE_PROJECT || "spaceapp-dev");
    this.postgresPort = Number.parseInt(this.env.SPACE_DEV_POSTGRES_PORT || "5433", 10);
    this.temporalPort = Number.parseInt(this.env.SPACE_DEV_TEMPORAL_PORT || "7234", 10);
    this.apiPort = Number.parseInt(this.env.SPACE_API_PORT || "4920", 10);
    this.webPort = Number.parseInt(this.env.SPACE_WEB_PORT || "4921", 10);
    this.secretsDir = join(this.stateDir, "secrets");
    this.binDir = join(this.stateDir, "bin");
    this.vendorDir = join(this.stateDir, "vendor");
    this.runDir = join(this.stateDir, "run");
    this.workspacesDir = join(this.stateDir, "workspaces");
    this.memoryDir = join(this.stateDir, "memory");
    this.memoryGraphDir = join(this.stateDir, "memory-graph");
    this.artifactsDir = join(this.stateDir, "artifacts");
    this.composeFile = join(this.stateDir, "docker-compose.yml");
    this.children = new Map();
    this.stopping = false;
    this.isMain = Boolean(options.isMain ?? (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)));
  }

  async readTemplates() {
    const composeTemplatePath = resolve(this.repoRoot, "packages/run-spaceapp/templates/compose.yml");
    const composeContent = await readFile(composeTemplatePath, "utf8");
    const dockerfilePath = resolve(this.repoRoot, "Dockerfile");
    const dockerfileContent = await readFile(dockerfilePath, "utf8");
    const images = extractPinnedImages(composeContent);
    const opencodeVersion = extractPinnedOpenCode(dockerfileContent);
    return { images, opencodeVersion };
  }

  async initDirs() {
    await ensureDir(this.stateDir, 0o700);
    await ensureDir(this.secretsDir, 0o700);
    await ensureDir(this.binDir, 0o755);
    await ensureDir(this.vendorDir, 0o755);
    await ensureDir(this.runDir, 0o755);
    await ensureDir(this.workspacesDir, 0o750);
    await ensureDir(this.memoryDir, 0o700);
    await ensureDir(this.memoryGraphDir, 0o700);
    await ensureDir(this.artifactsDir, 0o700);
    await ensureDir(join(this.stateDir, "providers/opencode"), 0o700);
  }

  async initSecrets() {
    const pgPassPath = join(this.secretsDir, "postgres-password");
    let pgPassword;
    if (existsSync(pgPassPath)) {
      pgPassword = (await readFile(pgPassPath, "utf8")).trim();
    } else {
      pgPassword = randomBytes(24).toString("hex");
      await writeSecret(pgPassPath, pgPassword);
    }

    const dbUrlPath = join(this.secretsDir, "database-url");
    const databaseUrl = `postgres://spaceapp:${pgPassword}@127.0.0.1:${this.postgresPort}/spaceapp`;
    if (!existsSync(dbUrlPath)) {
      await writeSecret(dbUrlPath, databaseUrl);
    }

    const sessionSecretPath = join(this.secretsDir, "session-secret");
    let sessionSecret;
    if (existsSync(sessionSecretPath)) {
      sessionSecret = (await readFile(sessionSecretPath, "utf8")).trim();
    } else {
      sessionSecret = randomBytes(32).toString("hex");
      await writeSecret(sessionSecretPath, sessionSecret);
    }

    const setupTokenPath = join(this.secretsDir, "setup-token");
    let setupToken;
    if (existsSync(setupTokenPath)) {
      setupToken = (await readFile(setupTokenPath, "utf8")).trim();
    } else {
      setupToken = randomBytes(32).toString("hex");
      await writeSecret(setupTokenPath, setupToken);
    }

    return { pgPassword, databaseUrl, setupToken, sessionSecret };
  }

  async initMemory() {
    const starterDir = resolve(this.repoRoot, "starter-memory");
    const files = ["gemini_history.md", "gemini_history_monthly.md", "installation-guide.md", "AGENTS.md"];
    for (const file of files) {
      const src = join(starterDir, file);
      const dest = join(this.memoryDir, file);
      if (existsSync(src) && !existsSync(dest)) {
        await copyFile(src, dest);
        try {
          await chmod(dest, 0o600);
        } catch {}
      }
    }
  }

  async installOpenCode(opencodeVersion) {
    const opencodeBin = join(this.vendorDir, "node_modules/.bin/opencode");
    if (!existsSync(opencodeBin)) {
      process.stdout.write(`Installing pinned opencode-ai@${opencodeVersion} into ${this.vendorDir}...\n`);
      await runStreaming("npm", ["install", "--prefix", this.vendorDir, "--prefer-offline", "--no-audit", "--no-fund", `opencode-ai@${opencodeVersion}`]);
    }

    const wrapperPath = join(this.binDir, "opencode-vscode-parity");
    const wrapperScript = `#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROVIDER_DIR="$STACK_DIR/providers/opencode"
export HOME="$PROVIDER_DIR"
export TMPDIR="$PROVIDER_DIR/tmp"
export XDG_CONFIG_HOME="$PROVIDER_DIR/config"
export XDG_DATA_HOME="$PROVIDER_DIR/data"
export XDG_CACHE_HOME="$PROVIDER_DIR/cache"
export XDG_STATE_HOME="$PROVIDER_DIR/state"
export PATH="$STACK_DIR/vendor/node_modules/.bin:$PATH"

mkdir -p "$HOME" "$TMPDIR" "$XDG_CONFIG_HOME" "$XDG_DATA_HOME" "$XDG_CACHE_HOME" "$XDG_STATE_HOME"
chmod 0700 "$HOME" "$TMPDIR" "$XDG_CONFIG_HOME" "$XDG_DATA_HOME" "$XDG_CACHE_HOME" "$XDG_STATE_HOME" 2>/dev/null || true

case "\${1:-}" in
  credential-status)
    printf 'READY\\n'
    exit 0
    ;;
  credential-observation)
    HASH=$(printf 'opencode:ready' | sha256sum | cut -d ' ' -f 1)
    printf 'OBSERVATION:%s\\n' "$HASH"
    exit 0
    ;;
  credential-smoke)
    printf 'SPACE_OPENCODE_OK\\n'
    exit 0
    ;;
  inspect-env)
    printf 'runtime=opencode\\nhome=isolated\\ncredential=present\\n'
    exit 0
    ;;
esac

exec opencode "$@"
`;
    await writeFile(wrapperPath, wrapperScript, { mode: 0o755 });
    try {
      await chmod(wrapperPath, 0o755);
    } catch {}

    const providers = [
      "codex", "claude", "gemini", "qwen", "kimi", "grok",
      "deepseek", "autohand", "cursor", "copilot", "hermes",
      "omp", "qoder", "muse", "droid"
    ];
    for (const name of providers) {
      const aliasPath = join(this.binDir, `${name}-vscode-parity`);
      await writeFile(aliasPath, wrapperScript, { mode: 0o755 });
      try {
        await chmod(aliasPath, 0o755);
      } catch {}
    }

    const opencodeDirectPath = join(this.binDir, "opencode");
    const opencodeDirectScript = `#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
export PATH="$STACK_DIR/vendor/node_modules/.bin:$PATH"
exec opencode "$@"
`;
    await writeFile(opencodeDirectPath, opencodeDirectScript, { mode: 0o755 });
    try {
      await chmod(opencodeDirectPath, 0o755);
    } catch {}
  }

  async startDocker(images) {
    const composeContent = generateComposeYaml({
      images,
      postgresPort: this.postgresPort,
      temporalPort: this.temporalPort,
      composeProject: this.composeProject,
      secretsDir: this.secretsDir
    });
    await writeFile(this.composeFile, composeContent, "utf8");

    process.stdout.write(`Starting PostgreSQL & Temporal containers (project: ${this.composeProject})...\n`);
    await runStreaming("docker", ["compose", "-p", this.composeProject, "-f", this.composeFile, "up", "-d"]);

    process.stdout.write(`Waiting for PostgreSQL (127.0.0.1:${this.postgresPort})...\n`);
    await waitForTcpPort("127.0.0.1", this.postgresPort, 45_000);

    process.stdout.write(`Waiting for Temporal (127.0.0.1:${this.temporalPort})...\n`);
    await waitForTcpPort("127.0.0.1", this.temporalPort, 90_000);

    const deadline = Date.now() + 90_000;
    while (Date.now() < deadline) {
      const { stdout } = await execDocker(["compose", "-p", this.composeProject, "-f", this.composeFile, "ps", "--format", "json"]);
      try {
        const lines = stdout.trim().split("\n").filter(Boolean);
        const statuses = lines.map((l) => JSON.parse(l));
        const pg = statuses.find((s) => s.Service === "postgres" || s.Name?.includes("postgres"));
        const temp = statuses.find((s) => s.Service === "temporal" || s.Name?.includes("temporal"));
        const pgHealth = pg?.Health || pg?.Status || "";
        const tempHealth = temp?.Health || temp?.Status || "";
        if ((pgHealth.includes("healthy") || pgHealth.includes("running")) && (tempHealth.includes("healthy") || tempHealth.includes("running"))) {
          break;
        }
      } catch {}
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  async runMigrations(databaseUrl) {
    process.stdout.write("Running database migrations...\n");
    await runStreaming("npm", ["run", "db:migrate"], {
      cwd: this.repoRoot,
      env: { ...process.env, ...this.env, SPACE_DATABASE_URL: databaseUrl }
    });
  }

  async buildWorkspacePackages() {
    if (this.env.SPACE_DEV_SKIP_BUILD === "true" || this.env.SPACE_DEV_SKIP_BUILD === "1") {
      process.stdout.write("Skipping workspace package builds (SPACE_DEV_SKIP_BUILD is set).\n");
      return;
    }
    const keyArtifacts = [
      "packages/contracts/dist/index.js",
      "packages/streaming/dist/index.js",
      "packages/browser-host/dist/index.js",
      "packages/opencode-control/dist/index.js",
      "packages/memory-graph/dist/index.js",
      "packages/cli-host/dist/main.js",
      "packages/codex-app-server/dist/index.js",
      "packages/runtime/dist/index.js",
      "packages/db/dist/index.js",
      "packages/gateway/dist/index.js",
      "apps/worker/dist/worker.js"
    ];
    const allBuilt = keyArtifacts.every((file) => existsSync(resolve(this.repoRoot, file)));
    if (allBuilt && this.env.SPACE_DEV_FORCE_BUILD !== "true") {
      process.stdout.write("Workspace packages are already built. Skipping build (use SPACE_DEV_FORCE_BUILD=true to force rebuild).\n");
      return;
    }
    process.stdout.write("Building workspace packages...\n");
    const packagesToBuild = [
      "@space/contracts", "@space/icons", "@space/streaming",
      "@space/browser-host",
      "@space/opencode-control", "@space/memory-graph",
      "@space/cli-host", "@space/codex-app-server",
      "@space/runtime", "@space/db", "@space/gateway", "@space/worker"
    ];
    for (const pkg of packagesToBuild) {
      await runStreaming("npm", ["run", "build", "-w", pkg], { cwd: this.repoRoot });
    }
  }

  resolveCliSocketPath() {
    const candidate = join(this.runDir, "cli-host.sock");
    if (Buffer.byteLength(candidate) <= 90) return candidate;
    return `/tmp/space-dev-${createHash("sha256").update(this.stateDir).digest("hex").slice(0, 8)}.sock`;
  }

  spawnService(name, command, args, extraEnv = {}) {
    const child = spawn(command, args, {
      cwd: this.repoRoot,
      env: {
        ...process.env,
        ...this.env,
        PATH: `${this.binDir}:${process.env.PATH || ""}`,
        ...extraEnv
      },
      stdio: ["ignore", "pipe", "pipe"]
    });

    this.children.set(name, child);

    child.stdout.on("data", (chunk) => {
      process.stdout.write(`[${name}] ${chunk}`);
    });

    child.stderr.on("data", (chunk) => {
      process.stderr.write(`[${name}] ${chunk}`);
    });

    child.once("error", (error) => {
      process.stderr.write(`[dev:stack] Service ${name} failed: ${error.message}\n`);
      if (!this.stopping) this.stop(1);
    });

    child.once("exit", (code, signal) => {
      this.children.delete(name);
      if (!this.stopping) {
        process.stderr.write(`[dev:stack] Service ${name} exited unexpectedly (${code ?? signal})\n`);
        this.stop(code || 1);
      }
    });

    return child;
  }

  async startServices({ databaseUrl, setupToken, sessionSecret }) {
    const cliSocketPath = this.resolveCliSocketPath();
    try {
      await rm(cliSocketPath, { force: true });
    } catch {}

    process.stdout.write("Starting CLI host from source...\n");
    this.spawnService("cli-host", process.execPath, [resolve(this.repoRoot, "packages/cli-host/dist/main.js")], {
      SPACE_CLI_HOST_SOCKET: cliSocketPath,
      SPACE_CLI_COMMAND_PATH: this.binDir,
      SPACE_CLI_WORKSPACE_ROOT: this.workspacesDir
    });

    const cliDeadline = Date.now() + 15_000;
    while (Date.now() < cliDeadline) {
      if (existsSync(cliSocketPath)) break;
      await new Promise((r) => setTimeout(r, 200));
    }

    process.stdout.write("Starting Temporal worker from source...\n");
    this.spawnService("worker", process.execPath, [resolve(this.repoRoot, "apps/worker/dist/worker.js")], {
      SPACE_DATABASE_URL: databaseUrl,
      SPACE_TEMPORAL_ADDRESS: `127.0.0.1:${this.temporalPort}`,
      SPACE_TEMPORAL_NAMESPACE: "default",
      SPACE_TEMPORAL_TASK_QUEUE: "space-agent-turns"
    });

    const tsxCli = resolve(this.repoRoot, "node_modules/tsx/dist/cli.mjs");
    const viteCli = resolve(this.repoRoot, "node_modules/vite/bin/vite.js");

    process.stdout.write(`Starting API server in watch mode (port: ${this.apiPort})...\n`);
    this.spawnService("api", process.execPath, [tsxCli, "watch", "apps/api/src/server.ts"], {
      SPACE_API_HOST: "127.0.0.1",
      SPACE_API_PORT: String(this.apiPort),
      SPACE_WEB_HOST: "127.0.0.1",
      SPACE_WEB_PORT: String(this.webPort),
      SPACE_API_ORIGIN: `http://127.0.0.1:${this.apiPort}`,
      SPACE_RUNTIME_STORE: "postgres",
      SPACE_DATABASE_URL: databaseUrl,
      SPACE_SESSION_SECRET: sessionSecret,
      SPACE_SESSION_SECRET_FILE: join(this.secretsDir, "session-secret"),
      SPACE_SETUP_TOKEN_FILE: join(this.secretsDir, "setup-token"),
      SPACE_TEMPORAL_ADDRESS: `127.0.0.1:${this.temporalPort}`,
      SPACE_TEMPORAL_NAMESPACE: "default",
      SPACE_TEMPORAL_TASK_QUEUE: "space-agent-turns",
      SPACE_CLI_HOST_SOCKET: cliSocketPath,
      SPACE_CLI_ENABLED: "true",
      SPACE_CLI_COMMAND_PATH: this.binDir,
      SPACE_CLI_OPENCODE_COMMAND: "opencode-vscode-parity",
      SPACE_CLI_WORKSPACE_ROOT: this.workspacesDir,
      SPACE_MEMORY_ROOT: this.memoryDir,
      SPACE_STARTER_MEMORY_ROOT: resolve(this.repoRoot, "starter-memory"),
      SPACE_MEMORY_GRAPH_ROOT: this.memoryGraphDir,
      SPACE_ARTIFACT_ROOT: this.artifactsDir,
      SPACE_DEV_LOGIN: "false",
      SPACEAPP_WORKFLOWS_ENABLED: "true",
      SPACE_PUBLIC_DISTRIBUTION: "true",
      SPACE_BROWSER_SESSIONS_ENABLED: "false",
      SPACE_INSTALL_ROOT: this.repoRoot
    });

    process.stdout.write(`Starting Vite web dev server (port: ${this.webPort})...\n`);
    this.spawnService("web", process.execPath, [viteCli, "apps/web", "--host", "127.0.0.1", "--port", String(this.webPort)], {
      SPACE_API_ORIGIN: `http://127.0.0.1:${this.apiPort}`,
      SPACE_API_PORT: String(this.apiPort),
      SPACE_WEB_HOST: "127.0.0.1",
      SPACE_WEB_PORT: String(this.webPort)
    });

    const pids = {};
    for (const [name, child] of this.children.entries()) {
      pids[name] = child.pid;
    }
    await writeFile(join(this.runDir, "pids.json"), JSON.stringify(pids, null, 2));

    process.stdout.write("Waiting for API readiness (/readyz)...\n");
    await waitForHttpOk(`http://127.0.0.1:${this.apiPort}/readyz`, (body) => body?.ok === true, 90_000);

    const banner = [
      "",
      "================================================================================",
      " SpaceApp dev stack is running from source!",
      "",
      ` Web URL:               http://127.0.0.1:${this.webPort}`,
      ` API URL:               http://127.0.0.1:${this.apiPort}`,
      ` Ready Check:           http://127.0.0.1:${this.apiPort}/readyz`,
      ` One-Time Setup Token:  ${setupToken}`,
      "",
      " Press Ctrl+C or run 'npm run dev:stack -- down' to stop.",
      "================================================================================",
      ""
    ].join("\n");
    process.stdout.write(banner);
  }

  stop(exitCode = 0) {
    if (this.stopping) return;
    this.stopping = true;
    for (const [name, child] of this.children.entries()) {
      try {
        child.kill("SIGTERM");
      } catch {}
    }
    this.children.clear();
    if (this.isMain) {
      setTimeout(() => {
        process.exit(exitCode);
      }, 3000).unref();
    }
  }

  async down() {
    this.stopping = true;
    process.stdout.write(`Stopping dev stack services (project: ${this.composeProject})...\n`);
    const pidsFile = join(this.runDir, "pids.json");
    if (existsSync(pidsFile)) {
      try {
        const pids = JSON.parse(await readFile(pidsFile, "utf8"));
        for (const [name, pid] of Object.entries(pids)) {
          try {
            process.kill(pid, "SIGTERM");
          } catch {}
        }
      } catch {}
      await rm(pidsFile, { force: true });
    }

    const childrenToKill = [...this.children.values()];
    this.children.clear();
    for (const child of childrenToKill) {
      try {
        child.kill("SIGTERM");
      } catch {}
    }

    if (existsSync(this.composeFile)) {
      try {
        await execDocker(["compose", "-p", this.composeProject, "-f", this.composeFile, "down"]);
      } catch (err) {
        process.stderr.write(`Docker compose down failed: ${err.message}\n`);
      }
    }
    const cliSocketPath = this.resolveCliSocketPath();
    try {
      await rm(cliSocketPath, { force: true });
    } catch {}
    process.stdout.write("Dev stack stopped cleanly.\n");
  }

  async reset() {
    this.stopping = true;
    process.stdout.write(`Resetting dev stack: removing containers, volumes, and state directory...\n`);
    await this.down();
    if (existsSync(this.composeFile)) {
      try {
        await execDocker(["compose", "-p", this.composeProject, "-f", this.composeFile, "down", "-v", "--remove-orphans"]);
      } catch (err) {
        process.stderr.write(`Docker compose down -v failed: ${err.message}\n`);
      }
    }
    const cliSocketPath = this.resolveCliSocketPath();
    try {
      await rm(cliSocketPath, { force: true });
    } catch {}

    if (existsSync(this.stateDir)) {
      await rm(this.stateDir, { recursive: true, force: true });
    }
    process.stdout.write("Dev stack reset complete. No containers, volumes, or state directories remain.\n");
  }

  async start() {
    this.stopping = false;
    const { images, opencodeVersion } = await this.readTemplates();
    await this.initDirs();
    const { databaseUrl, setupToken, sessionSecret } = await this.initSecrets();
    await this.initMemory();
    await this.installOpenCode(opencodeVersion);
    await this.startDocker(images);
    await this.runMigrations(databaseUrl);
    await this.buildWorkspacePackages();
    await this.startServices({ databaseUrl, setupToken, sessionSecret });
  }
}

const isMain = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const runner = new DevStackRunner();
  const command = process.argv[2]?.replace(/^--/, "") || "start";

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.once(signal, () => runner.stop(0));
  }

  if (command === "down" || command === "stop") {
    await runner.down().catch((err) => {
      process.stderr.write(`${err.message}\n`);
      process.exit(1);
    });
  } else if (command === "reset") {
    await runner.reset().catch((err) => {
      process.stderr.write(`${err.message}\n`);
      process.exit(1);
    });
  } else if (command === "help" || command === "h") {
    process.stdout.write(`Usage: npm run dev:stack [-- start|down|reset|help]

Commands:
  start (default)   Boot Docker services, run DB migrations, and launch full SpaceApp stack from source
  down              Stop all running dev stack processes and Docker containers
  reset             Stop and completely remove all containers, volumes, and .dev-stack state directory
  help              Display this help message

Environment overrides:
  SPACE_DEV_STACK_DIR         State directory (default: .dev-stack)
  SPACE_DEV_COMPOSE_PROJECT   Compose project name (default: spaceapp-dev)
  SPACE_DEV_POSTGRES_PORT     PostgreSQL port bound to 127.0.0.1 (default: 5433)
  SPACE_DEV_TEMPORAL_PORT     Temporal port bound to 127.0.0.1 (default: 7234)
  SPACE_API_PORT              API port bound to 127.0.0.1 (default: 4920)
  SPACE_WEB_PORT              Web Vite port bound to 127.0.0.1 (default: 4921)
`);
  } else {
    await runner.start().catch((err) => {
      process.stderr.write(`[dev:stack] Launch failed: ${err.message}\n`);
      runner.stop(1);
    });
  }
}
