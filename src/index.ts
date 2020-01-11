import { promisify } from 'util';
const sleep = promisify(setTimeout);
import { hash } from './hash';
import { Event, NewEvent } from './event';

import { Pool } from 'pg';

import { BookmarkManager } from './bookmarkManager';
import { init } from './init';

const connectionString = 'postgres://postgres:passw0rd@localhost:5432/postgres';

const pool = new Pool({
  connectionString,
});

export interface EventLedgetOptions {
  connectionString: string;
}

export function EventLedger(connectionString: string) {
  const pool = new Pool({
    connectionString,
  });

  return {
    read: async (options: ReadOptions) => {
      await init(pool);
      return read(options, pool, connectionString);
    },
    write: async (events: NewEvent[]) => {
      await init(pool);
      return write(events, pool);
    },
  };
}

const EVENT_TABLE = 'events';

interface ReadOptions {
  reader: string;
  process: (events: Event[]) => Promise<void>;
  onProcessError?: (error: Error, events: Event[]) => Promise<void>;
  limit?: number;
}

const DEFAULT_LIMIT = 200;
const DEFAULT_BOOKMARK_EXPIRATION_TIME = 10000;

export async function read(options: ReadOptions, pool: Pool, connectionString: string) {
  const { reader, process, onProcessError } = options;
  const limit = options.limit || DEFAULT_LIMIT;

  const bookmarkManager = await BookmarkManager({ reader, connectionString });

  async function _read() {
    const bookmark = await bookmarkManager.checkoutBookmark();
    if (!bookmark) {
      console.log(`[${reader}] no bookmarks left, sleeping for a bit...`);
      await sleep(2000);
      _read();
      return;
    }

    let bookmarkExpired = false;
    setTimeout(() => {
      bookmarkExpired = true;
    }, DEFAULT_BOOKMARK_EXPIRATION_TIME);

    const { partition } = bookmark;
    let { index } = bookmark;

    while (!bookmarkExpired) {
      const query = `
        SELECT *
        FROM ${EVENT_TABLE}
        WHERE id > $1 AND partition = $2
        ORDER BY id ASC
        LIMIT $3;`;

      const { rows: events } = await pool.query<Event>(query, [index, partition, limit]);

      try {
        await process(events);
      } catch (e) {
        onProcessError && (await onProcessError(e, events));
        break;
      }

      index = events[events.length - 1].index;
      await bookmarkManager.updateBookmark(partition, index);
    }

    await bookmarkManager.returnBookmark();
    _read();
  }

  _read();
}

export async function write(events: NewEvent[], pool: Pool) {
  const parameterPlaceholders: string[] = [];
  //TODO: fix typing
  const parameters: any[] = [];
  for (let i = 1; i <= events.length; i++) {
    parameterPlaceholders.push(`($${i}, $${i + 1})`);
    parameters.push(events[i - 1].payload);
    //TODO: generate partition key
    parameters.push(1);
  }

  const query = `
INSERT INTO ${EVENT_TABLE} 
  (payload, partition)
VALUES ${parameterPlaceholders.join(',')};
`;

  await pool.query(query, parameters);
}

const processAs = (consumer: string) => {
  return async (events: Event[]) => {
    console.log(
      `consumer ${consumer} is handing events ${events[0].id}-${
        events[events.length - 1].id
      }`
    );
    await sleep(1000);
    const random = Math.random();
    if (random < 0.9) {
      // throw new Error('asdf ' + random);
    }
  };
};

(async () => {
  setInterval(() => {
    write([
      {
        payload: {
          to: 'asdf',
          type: 'asdf',
        },
      },
    ]);
  }, 1000);

  read({
    reader: 'A',
    process: processAs('A1'),
    onProcessError: async (error, events) => {
      console.log(error.stack);
      console.log(`failed to process ${events.length} events`);
    },
  });

  const ss = [
    'asdf',
    'sadf',
    'this is a long string with spaces',
    'boggle',
    'boggle',
    'boggle',
  ];
  ss.forEach(s => console.log(`${s} -> ${hash(s)}`));
})();
