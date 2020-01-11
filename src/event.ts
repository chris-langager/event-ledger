export interface Event {
  index: number;
  date: Date;
  type?: string;
  aggregateType?: string;
  aggregateId?: string;
  actor?: string;
  payload?: object;
}

//I know Omit<...> is a thing, but this gives better type errors
export interface NewEvent {
  type?: string;
  aggregateType?: string;
  aggregateId?: string;
  actor?: string;
  payload?: object;
}
