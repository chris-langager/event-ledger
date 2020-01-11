import { DEFAULT_LIMIT } from './defaults';
import { Event, ReadFilters, EventRow } from './types';
import { Pool } from 'pg';
import { rowToEvent, filtersToSQL } from './utils';

//this is probably going to get out of hand for anyone who needs to lookup a lot of events
export async function lookUp(where: ReadFilters, pool: Pool): Promise<Event[]> {
  const { sql, bindArgs } = filtersToSQL(1, where);

  const query = `
  SELECT *
  FROM events
  ${sql ? 'WHERE ' + sql : ''}
  ORDER BY index ASC
  ;`;

  const { rows } = await pool.query<EventRow>(query, bindArgs);
  return rows.map(rowToEvent);
}

//CONSIDER - are generators well known enough to use them here?  would a stream be better?
export async function* lookUpGenerator(where: ReadFilters, pool: Pool) {
  const { sql, bindArgs } = filtersToSQL(3, where);

  let lastIndex = 0;

  while (true) {
    const query = `
  SELECT *
  FROM events
  WHERE index > $1
  ${sql ? 'AND ' + sql : ''}
  ORDER BY index ASC
  LIMIT $2;
  `;

    const { rows } = await pool.query<EventRow>(query, [
      lastIndex,
      DEFAULT_LIMIT,
      ...bindArgs,
    ]);
    const events = rows.map(rowToEvent);

    if (events.length === 0) {
      break;
    }

    yield events;
    lastIndex = events[events.length - 1].index;
  }
}
