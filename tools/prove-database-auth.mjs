import fs from "node:fs";
import path from "node:path";
import cp from "node:child_process";
import { fileURLToPath } from "node:url";

// Use only new synthetic databases; existing containers and volumes are untouched.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const directory = path.join(root, "backend/.cache");
fs.mkdirSync(directory, { recursive: true });
const project = "pulsechat-secure-proof-" + Date.now().toString(36);
const configFile = path.join(directory, project + ".json");
const call = (args) =>
  cp.execFileSync("docker", args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    timeout: 180000,
  });
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const rendered = JSON.parse(
  call([
    "compose",
    "-f",
    "docker-compose.yml",
    "-f",
    "docker-compose.secure.yml",
    "config",
    "--format",
    "json",
  ]),
);
const services = {
  mongo: rendered.services.mongo,
  redis: rendered.services.redis,
};
for (const service of Object.values(services)) {
  delete service.ports;
  delete service.networks;
  delete service.container_name;
}
fs.writeFileSync(
  configFile,
  JSON.stringify({
    name: project,
    services,
    volumes: { "mongo-data": {}, "redis-data": {} },
  }),
  { mode: 0o600 },
);
const compose = ["compose", "-p", project, "-f", configFile];
const result = {
  project,
  at: new Date().toISOString(),
  mongoUnauthRejected: false,
  mongoAuthAccepted: false,
  redisUnauthRejected: false,
  redisAuthAccepted: false,
  redisUnprivileged: false,
};
try {
  call([...compose, "up", "-d"]);
  const mongo = project + "-mongo-1";
  const redis = project + "-redis-1";
  const deadline = Date.now() + 240000;
  // The Mongo image temporarily opens TCP during initialization before auth is ready.
  const probe =
    'let rejected=false;try{new Mongo("mongodb://127.0.0.1:27017").getDB("admin").getCollection("system.users").countDocuments({});}catch(error){rejected=error.code===13;}print(JSON.stringify({accepted:db.runCommand({connectionStatus:1}).authInfo.authenticatedUsers.length===1,rejected}));';
  while (Date.now() < deadline) {
    try {
      const observed = JSON.parse(
        call([
          "exec",
          mongo,
          "sh",
          "-ec",
          'exec mongosh --quiet --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --eval "$1"',
          "sh",
          probe,
        ]),
      );
      result.mongoAuthAccepted = observed.accepted;
      result.mongoUnauthRejected = observed.rejected;
      if (observed.accepted && observed.rejected) break;
    } catch {
      // Readiness must include the authentication boundary, not just an open port.
    }
    await wait(1000);
  }
  result.redisUnauthRejected = /NOAUTH/.test(
    call(["exec", redis, "redis-cli", "ping"]),
  );
  result.redisAuthAccepted =
    call([
      "exec",
      redis,
      "sh",
      "-ec",
      'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli ping',
    ]).trim() === "PONG";
  const status = call(["exec", redis, "cat", "/proc/1/status"]);
  const uid = status.match(/^Uid:\s+(\d+)/m)?.[1];
  result.redisUnprivileged = uid !== undefined && uid !== "0";
  if (
    ![
      result.mongoAuthAccepted,
      result.mongoUnauthRejected,
      result.redisAuthAccepted,
      result.redisUnauthRejected,
      result.redisUnprivileged,
    ].every(Boolean)
  )
    throw new Error("A database authentication boundary failed");
  result.result = "PASS";
} catch {
  result.result = "FAIL";
  result.error =
    "The isolated database authentication proof failed; private command output is withheld.";
  process.exitCode = 1;
} finally {
  const owned = ["mongo", "redis"].every((service) => {
    try {
      const labels = JSON.parse(
        call([
          "inspect",
          "--format",
          "{{json .Config.Labels}}",
          project + "-" + service + "-1",
        ]),
      );
      return labels["com.docker.compose.project"] === project;
    } catch {
      return false;
    }
  });
  fs.writeFileSync(
    path.join(directory, "secure-proof-result.json"),
    JSON.stringify(result, null, 2) + "\n",
  );
  if (owned) {
    try {
      call([...compose, "down", "--volumes"]);
      result.cleanup = "completed";
    } catch {
      result.cleanup = "timed out; inspect this uniquely named test project";
      process.exitCode = 1;
    }
  }
  if (result.cleanup === "completed") fs.unlinkSync(configFile);
  fs.writeFileSync(
    path.join(directory, "secure-proof-result.json"),
    JSON.stringify(result, null, 2) + "\n",
  );
  console.log(JSON.stringify(result));
}
