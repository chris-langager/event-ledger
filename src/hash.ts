import { DEFAULT_PARTITIONS } from './defaults';
/*
used to find a partition number based on a string
(typically the aggregateId)

if no aggregateId is used, a random partition 
will be assigned
*/
export function hash(s?: string) {
  if (!s) {
    return Math.floor(Math.random() * DEFAULT_PARTITIONS) + 1;
  }

  const sumOfCharCodes = s
    .split('')
    .map(o => o.charCodeAt(0))
    .reduce((a, b) => a + b, 0);
  return (sumOfCharCodes % DEFAULT_PARTITIONS) + 1;
}
