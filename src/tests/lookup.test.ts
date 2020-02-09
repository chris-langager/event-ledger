import { createEventLedger } from '..';
import * as uuid from 'uuid';

it('can write and lookup an event', async () => {
  const connectionString = 'postgres://postgres:passw0rd@localhost:5432/postgres';
  const eventLedger = createEventLedger({ connectionString });

  const type = 'CustomerCreated';
  const aggregateType = 'Customer';
  const actor = 'system';
  const aggregateId = uuid.v4();
  const payload = {
    name: 'Susie',
  };
  await eventLedger.write([
    {
      type,
      aggregateType,
      aggregateId,
      actor,
      payload,
    },
  ]);

  const events = await eventLedger.lookUp({ aggregateIds: [aggregateId] });

  expect(events.length).toEqual(1);
  expect(events[0].aggregateType).toEqual(aggregateType);
  expect(events[0].type).toEqual(type);
  expect(events[0].actor).toEqual(actor);
  expect(events[0].payload).toEqual(payload);
});
