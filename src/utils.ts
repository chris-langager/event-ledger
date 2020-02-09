import { Filters, Event, EventRow } from './types';

export function rowToEvent(row: EventRow): Event {
  return {
    index: row.index,
    date: row.date_time,
    type: row.type,
    aggregateId: row.aggregate_id,
    aggregateType: row.aggregate_type,
    actor: row.actor,
    payload: row.payload,
  };
}

const keyToColumn: { [key in keyof Filters]: string } = {
  types: 'type',
  aggregateIds: 'aggregate_id',
  aggregateTypes: 'aggregate_type',
  actors: 'actor',
};

//helper function to translate filters into sql + bindArgs
export function filtersToSQL(startArgIndex: number, where?: Filters) {
  if (!where) {
    return { sql: '', bindArgs: [] };
  }
  let argIndex = startArgIndex;

  const sqlParts: string[] = [];
  const bindArgs: any[] = [];
  Object.keys(where)
    .filter(key => !!where[key])
    .forEach(key => {
      sqlParts.push(
        `${keyToColumn[key]} in (${Object.keys(where[key])
          .map(() => `$${argIndex++}`)
          .join(', ')})`
      );
      bindArgs.push(...where[key]);
    });

  return {
    sql: sqlParts.join(' AND '),
    bindArgs,
  };
}
