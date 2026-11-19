import { readdir, readFile, stat } from 'node:fs/promises';
import { dirname, resolve, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const ignored = new Set(['node_modules', '.git', '.cache', 'dist']);
const files = [];
async function walk(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (ignored.has(entry.name)) continue;
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) await walk(path);
    else if (entry.name.endsWith('.md')) files.push(path);
  }
}
await walk(root);
let checked = 0;
const failures = [];
const anchors = new Map();
async function headingIds(file) {
  if (anchors.has(file)) return anchors.get(file);
  const source = (await readFile(file, 'utf8')).replace(/```[\s\S]*?```/g, '');
  const ids = new Set();
  const counts = new Map();
  for (const match of source.matchAll(/^#{1,6}\s+(.+?)\s*#*\s*$/gm)) {
    const slug = match[1].toLowerCase().replace(/<[^>]*>/g, '')
      .replace(/[^\p{L}\p{N}_\- ]/gu, '').trim().replace(/\s/g, '-');
    const count = counts.get(slug) ?? 0;
    counts.set(slug, count + 1);
    ids.add(slug + (count ? '-' + count : ''));
  }
  for (const match of source.matchAll(/\b(?:id|name)=["']([^"']+)["']/g)) ids.add(match[1]);
  anchors.set(file, ids);
  return ids;
}
for (const file of files) {
  const source = (await readFile(file, 'utf8')).replace(/```[\s\S]*?```/g, '');
  const destinations = [
    ...source.matchAll(/!?\[[^\]]*\]\(<?([^\s)>]+)>?(?:\s+"[^"]*")?\)/g),
    ...source.matchAll(/^\s*\[[^\]]+\]:\s*<?([^\s>]+)>?/gm),
  ];
  for (const match of destinations) {
    const url = match[1];
    if (/^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(url)) continue;
    const path = decodeURIComponent(url.split(/[?#]/)[0]);
    const target = path ? resolve(dirname(file), path) : file;
    checked++;
    try {
      await stat(target);
      const hash = url.includes('#') ? decodeURIComponent(url.split('#')[1]) : '';
      if (hash && target.endsWith('.md') && !(await headingIds(target)).has(hash))
        throw new Error('Missing heading');
    } catch {
      failures.push(`${relative(root, file)}: ${url}`);
    }
  }
}
if (failures.length) {
  console.error(failures.join('\n'));
  process.exitCode = 1;
} else console.log(`PASS ${checked} relative documentation links in ${files.length} Markdown files`);
