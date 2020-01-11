import { Pool } from 'pg';
import { NewEvent } from './types';
import { hash } from './hash';

export async function write(events: NewEvent[], pool: Pool) {
  const parameterPlaceholders: string[] = [];
  const parameters: any[] = [];
  let row = 0;
  for (let i = 0; i < events.length; i++) {
    row = i * 6;
    parameterPlaceholders.push(
      `($${row + 1}, $${row + 2},$${row + 3}, $${row + 4},$${row + 5}, $${row + 6})`
    );

    const { type, aggregateType, aggregateId, actor, payload } = events[i];
    parameters.push(hash(aggregateId)); //partition
    parameters.push(type);
    parameters.push(aggregateType);
    parameters.push(aggregateId);
    parameters.push(actor);
    parameters.push(payload);
  }

  const query = `
  INSERT INTO events
    (partition, type, aggregate_type, aggregate_id, actor, payload)
  VALUES ${parameterPlaceholders.join(',')};
  `;

  await pool.query(query, parameters);
}
