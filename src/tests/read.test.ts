import { sleep } from './utils';
import { EventLedger, createEventLedger } from '..';
import * as uuid from 'uuid';

it('read test', async () => {
  const connectionString = 'postgres://postgres:passw0rd@localhost:5432/postgres';
  const eventLedger = createEventLedger({ connectionString });

  const actor = uuid.v4();
  for (let i = 0; i < 10; i++) {
    writeCreateCustomerEvent(eventLedger, actor);
  }

  let count = 0;
  eventLedger.read({
    reader: 'A',
    where: {
      actors: [actor],
    },
    process: async events => {
      events.forEach(event => count++);
    },
  });

  await sleep(3000);

  expect(count).toEqual(10);
});

async function writeCreateCustomerEvent(eventLedger: EventLedger, actor: string) {
  const type = 'CustomerCreated';
  const aggregateType = 'Customer';
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
}
