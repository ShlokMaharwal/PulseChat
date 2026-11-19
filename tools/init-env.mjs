import { randomBytes } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const file = resolve(root, ".env");
let source = await readFile(file, "utf8").catch(() => "");
let changed = false;
for (const key of ["JWT_SECRET", "MONGO_PASSWORD", "REDIS_PASSWORD"]) {
  const line = source
    .split(/\r?\n/)
    .find((value) => value.startsWith(key + "="));
  const value = line?.slice(key.length + 1) ?? "";
  const weakJwt =
    key === "JWT_SECRET" && (value.length < 32 || new Set(value).size < 12);
  if (
    value &&
    !weakJwt &&
    !/pulsechat-local|change|default|example|replace|test-secret/i.test(value)
  )
    continue;
  const replacement = key + "=" + randomBytes(32).toString("hex");
  if (line) source = source.replace(line, replacement);
  else source += "\n" + replacement;
  changed = true;
}
if (changed) await writeFile(file, source.trim() + "\n", { mode: 0o600 });
console.log(
  changed
    ? "Generated local secrets in ignored .env; existing non-default values were preserved."
    : "Existing non-default secrets were preserved.",
);
