export interface Event {
  index: number;
  date: Date;
  type: string;
  aggregateType: string;
  aggregateId: string;
  actor: string;
  payload: object;
}

//I know Omit<...> is a thing, but this gives better type errors
export interface NewEvent {
  type: string;
  aggregateType: string;
  aggregateId: string;
  actor: string;
  payload: object;
}

export interface EventRow {
  index: number;
  partition: number;
  date_time: Date;
  type: string;
  aggregate_type: string;
  aggregate_id: string;
  actor: string;
  payload: object;
}

export interface Filters {
  types?: string[];
  aggregateIds?: string[];
  aggregateTypes?: string[];
  actors?: string[];
}
