/*
used to find a partition number based on a string
(typically the aggregateId)

if no aggregateId is used, a random partition 
will be assigned
*/
export function hash(s?: string) {
  if (!s) {
    return Math.floor(Math.random() * 9) + 1;
  }

  const sumOfCharCodes = s
    .split('')
    .map(o => o.charCodeAt(0))
    .reduce((a, b) => a + b, 0);
  return (sumOfCharCodes % 9) + 1;
}
