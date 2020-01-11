import { BookmarkManager } from './BookmarkManager';
import { Event } from './event';
import { promisify } from 'util';
import { Pool } from 'pg';
const sleep = promisify(setTimeout);

const DEFAULT_LIMIT = 200;
const DEFAULT_BOOKMARK_EXPIRATION_TIME = 10000;

export interface ReadOptions {
  reader: string;
  process: (events: Event[]) => Promise<void>;
  onProcessError?: (error: Error, events: Event[]) => Promise<void>;
  limit?: number;
}

interface EventRow {
  index: number;
  partition: number;
  date_time: Date;
  type?: string;
  aggregate_type?: string;
  aggregate_id?: string;
  actor?: string;
  payload?: object;
}

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
          FROM events
          WHERE id > $1 AND partition = $2
          ORDER BY id ASC
          LIMIT $3;`;

      const { rows } = await pool.query<EventRow>(query, [index, partition, limit]);
      const events = rows.map(row => ({
        index: row.index,
        date: row.date_time,
        type: row.type,
        aggregateId: row.aggregate_id,
        aggregateType: row.aggregate_type,
        actor: row.actor,
        payload: row.payload,
      }));

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
