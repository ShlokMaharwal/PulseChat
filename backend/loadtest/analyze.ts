import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { MongoClient } from 'mongodb';
import { analyzeRun } from './report.js';
export async function inspectPersistence(directory: string) {
  const file = resolve(directory, 'measured.json');
  const measured = JSON.parse(await readFile(file, 'utf8')) as {
    runId?: string;
    sent?: { count: number };
    confirmed?: { count: number };
    [key: string]: unknown;
  };
  if (measured.runId) {
    console.log(JSON.stringify(await analyzeRun(directory)));
    return;
  }
  const fixture = JSON.parse(
    await readFile(resolve(directory, 'fixture.json'), 'utf8'),
  ) as { accounts: { chatId: string }[] };
  const mongo = new MongoClient(
    process.env.MONGO_URL ?? 'mongodb://localhost:27018',
    { serverSelectionTimeoutMS: 10000 },
  );
  try {
    await mongo.connect();
    measured.persistedAfterRun = await mongo
      .db(process.env.MONGO_DB ?? 'pulsechat')
      .collection('messages')
      .countDocuments({
        chatId: {
          $in: [...new Set(fixture.accounts.map((account) => account.chatId))],
        },
        body: { $regex: '^k6-socket-' },
      });
    measured.persistenceObservedAt = new Date().toISOString();
    measured.unconfirmedAtShutdown =
      measured.sent && measured.confirmed
        ? measured.sent.count - measured.confirmed.count
        : null;
    await writeFile(file, JSON.stringify(measured, null, 2) + '\n');
    console.log(
      JSON.stringify({
        result: 'Persistence observation recorded',
        sent: measured.sent?.count,
        confirmed: measured.confirmed?.count,
        stored: measured.persistedAfterRun,
        observedAt: measured.persistenceObservedAt,
      }),
    );
  } finally {
    await mongo.close();
  }
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  if (!process.argv[2]) throw new Error('Supply the measured run directory');
  await inspectPersistence(process.argv[2]);
}
