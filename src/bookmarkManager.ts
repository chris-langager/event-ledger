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

  return {
    checkoutBookmark: () => checkoutBookmark(client, reader),
    updateBookmark: (partition: number, index: number) =>
      updateBookmark(client, reader, partition, index),
    returnBookmark: () => returnBookmark(client),
  };
}

async function checkoutBookmark(
  client: Client,
  reader: string
): Promise<Bookmark | null> {
  client.query('BEGIN;');

  const query = `
select *
from bookmarks
where consumer = $1
order by date_returned3 asc, index asc, partition asc
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
where consumer = $2 and partition = $3;
`;

  await client.query(query, [index, reader, partition]);
}

async function returnBookmark(client: Client) {
  const query = `
COMMIT;
`;

  await client.query(query);
}
