import { promisify } from 'util';
const sleep = promisify(setTimeout);

import { Event, NewEvent } from './event';

import { Pool } from 'pg';

import { init } from './init';
import { read, ReadOptions } from './read';
import { write } from './write';

export interface EventLedgetOptions {
  connectionString: string;
}

export function EventLedger(options: EventLedgetOptions) {
  const { connectionString } = options;
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

const processAs = (reader: string) => {
  return async (events: Event[]) => {
    console.log(
      `reader ${reader} is handing events ${events[0].index}-${
        events[events.length - 1].index
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
  const connectionString = 'postgres://postgres:passw0rd@localhost:5432/postgres';

  const eventLedger = EventLedger({ connectionString });
  //   eventLedger.write([
  //     {
  //       payload: {
  //         to: 'asdf',
  //         type: 'asdf',
  //       },
  //     },
  //   ]);

  setInterval(() => {
    eventLedger.write([
      {
        type: 'userLoggedIn',
        // aggregateId: '123',
        payload: {
          username: 'asdf',
        },
      },
    ]);
  }, 1000);

  for (let i = 0; i < 1; i++) {
    eventLedger.read({
      reader: 'A' + i,
      where: {
        types: ['userLoggedIn'],
        aggregateIds: ['asdf', '123'],
      },
      process: processAs(`A${i}`),
      onProcessError: async (error, events) => {
        console.log(error.stack);
        console.log(`failed to process ${events.length} events`);
      },
    });
  }

  //   const ss = [
  //     'asdf',
  //     'sadf',
  //     'this is a long string with spaces',
  //     'boggle',
  //     'boggle',
  //     'boggle',
  //   ];
  //   ss.forEach(s => console.log(`${s} -> ${hash(s)}`));
})();
