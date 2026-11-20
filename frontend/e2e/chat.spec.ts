import { test, expect, type BrowserContext, type Page } from '@playwright/test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const exec = promisify(execFile);
const password = 'Browser-test-password!';
const suffix = Date.now().toString(36);
const names = ['maya_' + suffix, 'jules_' + suffix] as const;
let contexts: BrowserContext[], pages: Page[];
const welcomes: [{ gatewayId: string }[], { gatewayId: string }[]] = [[], []];
let restoreService: string | undefined;
const errors: string[] = [];
async function send(page: Page, body: string) {
  await page.getByRole('textbox', { name: 'Message', exact: true }).fill(body);
  await page.getByRole('button', { name: 'Send message', exact: true }).click();
}
function message(page: Page, body: string) {
  return page.locator('article[data-message-id]').filter({ hasText: body });
}
test.describe.serial('real chat and recovery', () => {
  test.beforeAll(async ({ browser }) => {
    const baseURL = process.env.TEST_HTTP_URL ?? 'http://127.0.0.1:8081';
    contexts = await Promise.all([
      browser.newContext({ baseURL, colorScheme: 'dark' }),
      browser.newContext({ baseURL, colorScheme: 'light' }),
    ]);
    pages = await Promise.all(contexts.map((context) => context.newPage()));
    pages.forEach((page, index) => {
      page.on('pageerror', (error) => errors.push(error.message));
      page.on('websocket', (socket) =>
        socket.on('framereceived', (event) => {
          try {
            const frame = JSON.parse(String(event.payload));
            if (frame.type === 'welcome') welcomes[index as 0 | 1].push(frame);
          } catch {
            /* Non-JSON frames do not identify the session. */
          }
        }),
      );
    });
  });
  test.afterAll(async () => {
    if (restoreService)
      await exec('docker', ['compose', 'start', restoreService], { cwd: '..' });
    await Promise.all(contexts?.map((context) => context.close()) ?? []);
  });
  test('registration, login, theme, and keyboard navigation', async () => {
    for (let index = 0; index < 2; index++) {
      const page = pages[index]!;
      await page.goto('/');
      if (index === 0)
        await page.screenshot({ path: '../backend/.cache/auth-refined.png' });
      await page
        .getByRole('button', { name: 'Create an account', exact: true })
        .click();
      await page.getByLabel('Username', { exact: true }).fill(names[index]!);
      await page.getByLabel('Password', { exact: true }).fill(password);
      if (index === 0) {
        await expect(
          page.getByLabel('Password', { exact: true }),
        ).toHaveAttribute('type', 'password');
        await page
          .getByRole('button', { name: 'Show password', exact: true })
          .click();
        await expect(
          page.getByLabel('Password', { exact: true }),
        ).toHaveAttribute('type', 'text');
        await expect(page.getByLabel('Password', { exact: true })).toHaveValue(
          password,
        );
        await page
          .getByRole('button', { name: 'Hide password', exact: true })
          .click();
        await expect(
          page.getByLabel('Password', { exact: true }),
        ).toHaveAttribute('type', 'password');
      }
      await page
        .getByRole('button', { name: 'Create account', exact: true })
        .click();
      await expect(
        page.getByRole('heading', { name: 'Messages', exact: true }),
      ).toBeVisible();
      await expect
        .poll(() => welcomes[index as 0 | 1].length)
        .toBeGreaterThan(0);
    }
    const page = pages[0]!;
    expect(
      await page.evaluate(() =>
        getComputedStyle(document.documentElement)
          .getPropertyValue('--background')
          .trim(),
      ),
    ).toMatch(/^#(?:000|000000)$/);
    await page.getByRole('button', { name: 'Switch to light theme' }).click();
    await page.reload();
    await expect(
      page.getByRole('button', { name: 'Switch to dark theme' }),
    ).toBeVisible();
    await page.getByRole('button', { name: 'Switch to dark theme' }).click();
    const opener = page
      .locator('.sidebar-header')
      .getByRole('button', { name: 'New conversation', exact: true });
    await opener.click();
    await expect(page.getByLabel('Find someone by username')).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(opener).toBeFocused();
    await pages[1]!
      .getByRole('button', { name: 'Sign out', exact: true })
      .click();
    await pages[1]!.getByLabel('Username', { exact: true }).fill(names[1]!);
    await pages[1]!.getByLabel('Password', { exact: true }).fill(password);
    await pages[1]!
      .getByRole('button', { name: 'Sign in', exact: true })
      .click();
    await expect(
      pages[1]!.getByRole('heading', { name: 'Messages', exact: true }),
    ).toBeVisible();
  });
  test('sending, escaped text, receipts, days, and unread navigation', async () => {
    const a = pages[0]!,
      b = pages[1]!;
    await a
      .locator('.sidebar-header')
      .getByRole('button', { name: 'New conversation', exact: true })
      .click();
    await a.getByLabel('Find someone by username').fill(names[1]!);
    await a
      .getByRole('dialog')
      .getByRole('button')
      .filter({ hasText: names[1] })
      .click();
    await send(a, 'Hello from the first browser');
    await expect(message(a, 'Hello from the first browser')).toBeVisible();
    await expect(b.locator('.unread-badge')).toBeVisible();
    await b.locator('.chat-row').filter({ hasText: names[0] }).click();
    await expect(message(b, 'Hello from the first browser')).toBeVisible();
    await expect(
      message(a, 'Hello from the first browser').getByLabel('Read', {
        exact: true,
      }),
    ).toBeVisible();
    await expect(b.getByText('Unread messages', { exact: true })).toBeVisible();
    await a
      .getByRole('textbox', { name: 'Message', exact: true })
      .fill('Writing a reply');
    await expect(
      b.locator('.conversation-header').getByRole('status'),
    ).toContainText(`${names[0]} is typing`);
    await a.getByRole('textbox', { name: 'Message', exact: true }).fill('');
    await expect(
      b.locator('.conversation-header').getByRole('status'),
    ).not.toContainText('typing');
    const body = '<img src=x onerror=alert(1)> is ordinary chat text';
    await send(b, body);
    await expect(message(a, body)).toBeVisible();
    expect(await message(a, body).locator('img').count()).toBe(0);
    await expect(a.locator('.day-divider')).toHaveCount(1);
  });
  test('offline queuing preserves IDs and recovers ordered messages', async () => {
    const a = pages[0]!,
      b = pages[1]!;
    await contexts[1]!.setOffline(true);
    await expect(
      b.getByText('You are offline. Messages will send when you reconnect.'),
    ).toBeVisible();
    await send(b, 'Queued while offline');
    await expect(b.locator('.pending')).toContainText('Queued');
    await send(a, 'Sent while the peer was offline');
    await contexts[1]!.setOffline(false);
    await expect(message(a, 'Queued while offline')).toBeVisible();
    await expect(message(b, 'Sent while the peer was offline')).toBeVisible();
    await expect(b.locator('.pending')).toHaveCount(0);
    const ids = await b
      .locator('article[data-message-id]')
      .evaluateAll((nodes) =>
        nodes.map((node) => node.getAttribute('data-message-id')),
      );
    expect(new Set(ids).size).toBe(ids.length);
  });
  test('gateway SIGKILL reconnects to a healthy replica and recovers', async () => {
    const gateway = welcomes[1].at(-1)!.gatewayId;
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
    expect(service).toBeTruthy();
    restoreService = service;
    const count = welcomes[1].length;
    try {
      await exec('docker', ['compose', 'kill', '-s', 'SIGKILL', service!], {
        cwd: '..',
      });
      await send(pages[0]!, 'Recovered after the gateway stopped');
      await expect.poll(() => welcomes[1].length).toBeGreaterThan(count);
      await expect(
        message(pages[1]!, 'Recovered after the gateway stopped'),
      ).toBeVisible();
    } finally {
      await exec('docker', ['compose', 'start', service!], { cwd: '..' });
      restoreService = undefined;
    }
  });
  test('silent renewal and server-confirmed logout revoke the refresh family', async () => {
    const page = pages[1]!;
    await page.evaluate(() => {
      const account = JSON.parse(sessionStorage.getItem('pulsechat.account')!);
      const parts = account.token.split('.');
      const claims = JSON.parse(atob(parts[1]));
      claims.exp = 1;
      parts[1] = btoa(JSON.stringify(claims));
      account.token = parts.join('.');
      sessionStorage.setItem('pulsechat.account', JSON.stringify(account));
    });
    await page.reload();
    await expect(
      page.getByRole('heading', { name: 'Messages', exact: true }),
    ).toBeVisible();
    await expect
      .poll(() =>
        page.evaluate(
          () =>
            JSON.parse(
              atob(
                JSON.parse(
                  sessionStorage.getItem('pulsechat.account')!,
                ).token.split('.')[1],
              ),
            ).exp,
        ),
      )
      .toBeGreaterThan(Date.now() / 1000);
    const refreshToken = await page.evaluate(
      () =>
        JSON.parse(sessionStorage.getItem('pulsechat.account')!)
          .refreshToken as string,
    );
    await page.getByRole('button', { name: 'Sign out', exact: true }).click();
    await expect(
      page.getByRole('button', { name: 'Sign in', exact: true }),
    ).toBeVisible();
    const response = await page.request.post('/api/auth/refresh', {
      data: { refreshToken },
    });
    expect(response.status()).toBe(401);
    expect(errors).toEqual([]);
  });
  test('creates a three-person group with shared messages, typing, and mobile layout', async ({
    browser,
  }) => {
    const a = pages[0]!,
      b = pages[1]!;
    await b.getByLabel('Username', { exact: true }).fill(names[1]!);
    await b.getByLabel('Password', { exact: true }).fill(password);
    await b.getByRole('button', { name: 'Sign in', exact: true }).click();
    await expect(
      b.getByRole('heading', { name: 'Messages', exact: true }),
    ).toBeVisible();
    const context = await browser.newContext({
      baseURL: process.env.TEST_HTTP_URL ?? 'http://127.0.0.1:8081',
      colorScheme: 'light',
    });
    const c = await context.newPage();
    const thirdName = 'rin_' + suffix;
    const groupName = 'Project team ' + suffix;
    try {
      await c.goto('/');
      await c
        .getByRole('button', { name: 'Create an account', exact: true })
        .click();
      await c.getByLabel('Username', { exact: true }).fill(thirdName);
      await c.getByLabel('Password', { exact: true }).fill(password);
      await c
        .getByRole('button', { name: 'Create account', exact: true })
        .click();
      await expect(
        c.getByRole('heading', { name: 'Messages', exact: true }),
      ).toBeVisible();
      await a
        .locator('.sidebar-header')
        .getByRole('button', { name: 'New conversation', exact: true })
        .click();
      await a.getByRole('button', { name: 'New group', exact: true }).click();
      await a.getByLabel('Group name', { exact: true }).fill(groupName);
      for (const name of [names[1]!, thirdName]) {
        await a.getByLabel('Find someone by username').fill(name);
        await a
          .getByRole('dialog')
          .locator('.user-result')
          .filter({ hasText: name })
          .click();
      }
      await a
        .getByRole('button', { name: 'Create group · 3 members', exact: true })
        .click();
      await expect(
        a.getByRole('heading', { name: groupName, exact: true }),
      ).toBeVisible();
      for (const page of [b, c]) {
        await page.locator('.chat-row').filter({ hasText: groupName }).click();
        await expect(
          page.getByRole('heading', { name: groupName, exact: true }),
        ).toBeVisible();
        await expect(page.locator('.conversation-header')).toContainText(
          '3 members',
        );
      }
      const body = 'A message for the whole group';
      await a.getByRole('textbox', { name: 'Message', exact: true }).fill(body);
      for (const page of [b, c])
        await expect(
          page.locator('.conversation-header').getByRole('status'),
        ).toContainText(`${names[0]} is typing`);
      await a
        .getByRole('button', { name: 'Send message', exact: true })
        .click();
      for (const page of [a, b, c])
        await expect(message(page, body)).toBeVisible();
      await expect(
        message(a, body).getByLabel('Read', { exact: true }),
      ).toBeVisible();
      await send(b, 'A second group member replies');
      await expect(message(c, 'A second group member replies')).toBeVisible();
      await send(c, 'The third member is here too');
      await expect(message(a, 'The third member is here too')).toBeVisible();
      await context.setOffline(true);
      await expect(
        c.getByText('You are offline. Messages will send when you reconnect.'),
      ).toBeVisible();
      await send(a, 'Group history catches up after reconnect');
      await expect(
        message(b, 'Group history catches up after reconnect'),
      ).toBeVisible();
      await context.setOffline(false);
      await expect(
        message(c, 'Group history catches up after reconnect'),
      ).toBeVisible();
      expect(
        await message(a, body)
          .locator('p')
          .evaluate((node) => getComputedStyle(node).fontSize),
      ).toBe('16px');
      await a.screenshot({ path: '../backend/.cache/group-dark.png' });
      await c.setViewportSize({ width: 390, height: 844 });
      await expect(
        c.getByRole('heading', { name: groupName, exact: true }),
      ).toBeVisible();
      expect(
        await c.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
      await c.screenshot({ path: '../backend/.cache/group-mobile-light.png' });
    } finally {
      await context.close();
    }
  });
});
