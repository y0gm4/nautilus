import { Nautilus } from './jsclient/index.js';

const db = new Nautilus({ pool: { maxConnections: 1 } });
await db.connect();

const seen = [];

try {
  for await (const row of db.user.streamMany({ orderBy: { id: 'asc' }, chunkSize: 1 })) {
    seen.push(row.name);
    if (seen.length === __BREAK_AFTER__) {
      break;
    }
  }

  await new Promise((resolve) => setTimeout(resolve, 100));

  const follow = await db.user.findMany({ orderBy: { id: 'asc' }, take: 5 });
  const tail = await db.user.findMany({ orderBy: { id: 'asc' }, skip: __TAIL_SKIP__, take: 1 });

  console.log(`count=${seen.length}`);
  console.log(`first=${seen[0]}`);
  console.log(`tenth=${seen.at(-1)}`);
  console.log(`follow=${follow.map((row) => row.name).join(',')}`);
  console.log(`tail=${tail[0].name}`);
  console.log(`partialData=${db.partialData.size}`);
  console.log(`streams=${db.streams.size}`);
} finally {
  await db.disconnect();
}
