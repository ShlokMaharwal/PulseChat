import { expect, it } from 'vitest';
import { parseStatsLine } from '../../loadtest/telemetry.js';
it('retains the first container row after Docker CLI screen-control sequences', () => {
  const row = { Name: 'pulsechat-gateway-2-1', CPUPerc: '11.22%' };
  const escape = String.fromCharCode(27);
  expect(
    parseStatsLine(escape + '[2J' + escape + '[1;1H' + JSON.stringify(row)),
  ).toEqual(row);
  expect(parseStatsLine(JSON.stringify(row))).toEqual(row);
  expect(parseStatsLine('')).toBeUndefined();
});
