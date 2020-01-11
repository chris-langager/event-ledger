import { Client } from 'pg';

interface Bookmark {
  reader: string;
  partition: number;
  index: number;
}

interface BookmarkManagerOptions {
  connectionString: string;
  reader: string;
}
export async function BookmarkManager(options: BookmarkManagerOptions) {
  const { connectionString, reader } = options;
  const client = new Client({ connectionString });
  await client.connect();

  let partition = 0;

  return {
    checkoutBookmark: async () => {
      await ensureBookmarkExists(client, reader);
      const bookmark = await checkoutBookmark(client, reader);
      if (bookmark) partition = bookmark.partition;
      return bookmark;
    },
    updateBookmark: (index: number) => updateBookmark(client, reader, partition, index),
    returnBookmark: () => returnBookmark(client, reader, partition),
  };
}

async function ensureBookmarkExists(client: Client, reader: string) {
  const query = `
  insert into bookmarks
    (reader, partition)
values ${[...Array(8).keys()].map(i => i + 1).map(i => `('${reader}', ${i})`)}
       on conflict do nothing;  
  `;

  await client.query(query);
}

async function checkoutBookmark(
  client: Client,
  reader: string
): Promise<Bookmark | null> {
  client.query('BEGIN;');

  const query = `
select *
from bookmarks
where reader = $1
order by date_returned asc nulls first, index asc, partition asc
limit 1
    for update skip locked;
`;

  const { rows } = await client.query(query, [reader]);
  if (rows.length === 0) {
    return null;
  }
  return {
    reader,
    partition: rows[0].partition,
    index: rows[0].index,
  };
}

async function updateBookmark(
  client: Client,
  reader: string,
  partition: number,
  index: number
) {
  const query = `
update bookmarks
set index=$1
where reader = $2 and partition = $3;
`;

  await client.query(query, [index, reader, partition]);
}

async function returnBookmark(client: Client, reader: string, partition: number) {
  const query = `
update bookmarks
set date_returned=(now() at time zone 'utc')
where reader = $1 and partition = $2;
`;
  await client.query(query, [reader, partition]);
  await client.query('COMMIT;');
}
