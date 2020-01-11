import { promisify } from 'util';
import { Pool } from 'pg';
const sleep = promisify(setTimeout);

/*
make sure the essential tables are created in postgres
*/

const query = `
CREATE TABLE IF NOT EXISTS events
(
    index          SERIAL PRIMARY KEY,
    partition      SMALLINT NOT NULL,
    date_time      TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc'),
    type           TEXT,
    aggregate_type TEXT,
    aggregate_id   TEXT,
    actor          TEXT,
    payload        JSON
);

CREATE TABLE IF NOT EXISTS bookmarks
(
    reader        TEXT     NOT NULL,
    partition     SMALLINT NOT NULL,
    index         INTEGER  NOT NULL DEFAULT 0,
    date_returned TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (reader, partition)
);


`;

/*
All of this state is to make sure we don't span the DB
with queries to create tables.  We only want one query
to be out at a time.
*/
let initSuccess = false;
let queryRunning = false;
export async function init(pool: Pool) {
  //if we've already initialized, then this is a no-op
  if (initSuccess) {
    return;
  }

  //if init has already been called, wait a bit and try again
  if (queryRunning) {
    await sleep(50);
    return init(pool);
  }

  //this will be set by the first execution of this function
  queryRunning = true;
  try {
    await pool.query(query);
    initSuccess = true;
  } finally {
    queryRunning = false;
  }
}
