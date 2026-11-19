import { chromium } from '@playwright/test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const exec = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const directory = resolve(root, 'backend/.cache/demo');
await mkdir(directory, { recursive: true });
const browser = await chromium.launch({
  headless: true,
  channel: process.env.PLAYWRIGHT_CHANNEL ?? 'msedge',
});
const baseURL = process.env.TEST_HTTP_URL ?? 'http://127.0.0.1:8081';
const contexts = await Promise.all(
  ['dark', 'light'].map((colorScheme) =>
    browser.newContext({
      baseURL,
      colorScheme,
      viewport: { width: 800, height: 620 },
    }),
  ),
);
const pages = await Promise.all(contexts.map((context) => context.newPage()));
const welcomes = [[], []],
  errors = [],
  frames = [];
let restore;
pages.forEach((page, index) => {
  page.setDefaultTimeout(120000);
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('websocket', (socket) =>
    socket.on('framereceived', (event) => {
      try {
        const frame = JSON.parse(String(event.payload));
        if (frame.type === 'welcome') welcomes[index].push(frame);
      } catch {
        /* Only JSON welcome frames identify a gateway. */
      }
    }),
  );
});
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate) {
  const deadline = Date.now() + 90000;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw Error('Demo recovery timed out');
    await wait(100);
  }
}
let phase = 'Two real browser sessions exchanging messages';
const captions = [];
async function capture() {
  const index = frames.length;
  const paths = [
    resolve(directory, `a-${index}.png`),
    resolve(directory, `b-${index}.png`),
  ];
  await Promise.all(
    pages.map((page, i) => page.screenshot({ path: paths[i] })),
  );
  frames.push(paths);
  captions.push(phase);
}
async function send(page, body) {
  await page.getByRole('textbox', { name: 'Message', exact: true }).fill(body);
  await page.getByRole('button', { name: 'Send message', exact: true }).click();
}
async function canonical(page, body) {
  await page
    .locator('article[data-message-id]')
    .filter({ hasText: body })
    .waitFor();
}
const suffix = Date.now().toString(36),
  names = ['maya_' + suffix, 'jules_' + suffix];
try {
  for (let i = 0; i < 2; i++) {
    const page = pages[i];
    await page.goto('/');
    await page
      .getByRole('button', { name: 'Create an account', exact: true })
      .click();
    await page.getByLabel('Username', { exact: true }).fill(names[i]);
    await page
      .getByLabel('Password', { exact: true })
      .fill('Demo-browser-password!');
    await page
      .getByRole('button', { name: 'Create account', exact: true })
      .click();
    await page
      .getByRole('heading', { name: 'Messages', exact: true })
      .waitFor();
    await until(() => welcomes[i].length > 0);
  }
  await pages[0]
    .locator('.sidebar-header')
    .getByRole('button', { name: 'New conversation', exact: true })
    .click();
  await pages[0].getByLabel('Find someone by username').fill(names[1]);
  await pages[0]
    .getByRole('dialog')
    .getByRole('button')
    .filter({ hasText: names[1] })
    .click();
  await send(pages[0], 'Hey Jules, ready to catch up?');
  await canonical(pages[0], 'Hey Jules, ready to catch up?');
  await pages[1].locator('.chat-row').filter({ hasText: names[0] }).click();
  await canonical(pages[1], 'Hey Jules, ready to catch up?');
  await capture();
  await send(pages[1], 'Absolutely. Good to hear from you.');
  await canonical(pages[0], 'Absolutely. Good to hear from you.');
  await capture();
  await wait(500);
  await capture();
  const gateway = welcomes[1].at(-1).gatewayId;
  const inspected = await exec('docker', [
    'inspect',
    '--format',
    '{{.Config.Hostname}} {{index .Config.Labels "com.docker.compose.service"}}',
    'pulsechat-gateway-1-1',
    'pulsechat-gateway-2-1',
    'pulsechat-gateway-3-1',
  ]);
  const service = inspected.stdout
    .trim()
    .split(/\r?\n/)
    .map((line) => line.split(' '))
    .find(([id]) => id === gateway)?.[1];
  if (!service) throw Error('Could not identify the connected gateway');
  restore = () => exec('docker', ['compose', 'start', service], { cwd: root });
  const welcomeCount = welcomes[1].length;
  await exec('docker', ['compose', 'kill', '-s', 'SIGKILL', service], {
    cwd: root,
  });
  phase = `${service} received SIGKILL; waiting for conversation recovery`;
  await capture();
  await send(pages[0], 'Still here. Your conversation will catch up.');
  for (let i = 0; i < 6; i++) {
    await capture();
    await wait(500);
  }
  await until(() => welcomes[1].length > welcomeCount);
  await canonical(pages[1], 'Still here. Your conversation will catch up.');
  await send(pages[1], 'Back already—and nothing is missing.');
  await canonical(pages[0], 'Back already—and nothing is missing.');
  phase = `Reconnected to ${welcomes[1].at(-1).gatewayId}; all four messages recovered`;
  await restore();
  restore = undefined;
  for (let i = 0; i < 4; i++) {
    await capture();
    await wait(500);
  }
  if (errors.length) throw Error('Browser page errors: ' + errors.join(', '));
  const manifest = {
    frames,
    captions,
    output: resolve(root, 'docs/img/pulsechat-demo.gif'),
  };
  await writeFile(resolve(directory, 'frames.json'), JSON.stringify(manifest));
  await Promise.all(contexts.map((context) => context.close()));
  await browser.close();
  const encoded = await exec(process.env.PYTHON_BIN ?? 'python', [
    resolve(root, 'tools/encode-demo.py'),
    resolve(directory, 'frames.json'),
  ]);
  const proof = {
    result: 'PASS',
    at: new Date().toISOString(),
    realBrowserSessions: 2,
    logicalMessages: 4,
    killedGateway: service,
    previousGatewayId: gateway,
    recoveredGatewayId: welcomes[1].at(-1).gatewayId,
    pageErrors: errors,
    encoding: JSON.parse(encoded.stdout),
  };
  await writeFile(
    resolve(root, 'backend/.cache/demo-proof.json'),
    JSON.stringify(proof, null, 2) + '\n',
  );
  console.log(JSON.stringify(proof));
} catch (error) {
  for (let i = 0; i < pages.length; i++) {
    await pages[i]
      .screenshot({ path: resolve(directory, `failure-${i}.png`) })
      .catch(() => {});
    const notice = await pages[i]
      .locator('.form-error')
      .allTextContents()
      .catch(() => []);
    console.error('Browser', i, 'notice:', notice);
  }
  throw error;
} finally {
  if (restore) await restore();
  await browser.close();
}
