#!/usr/bin/env node

const moment = require('moment-timezone');

const parse = require('csv-parse');
const transform = require('stream-transform');
const stringify = require('csv-stringify');

const parser = parse({ info: true });
parser.on('error', err => console.error(err));

const transformer = transform(function(data) {
  // record is the actual row data; info is metadata about the row
  let { record, info } = data;
  if(info.records === 0) {
    // it's the header row; don't transform it
    return record;
  }

  try {
    return transformRow(record);
  } catch(e) {
    console.error(`Error transforming line ${info.lines}: ${e.message}`);
    return null; // remove this row from the output
  }
});
transformer.on('error', err => console.error(err));

const stringifier = stringify();
stringifier.on('error', err => console.error(err));

process.stdin.pipe(parser).pipe(transformer).pipe(stringifier).pipe(process.stdout);

const columnTransforms = [
  transformTimestamp,     // Timestamp
  transformFreeInput,     // Address
  transformZIP,           // ZIP
  transformFullName,      // FullName
  transformDuration,      // FooDuration
  transformDuration,      // BarDuration
  transformTotalDuration, // TotalDuration
  transformFreeInput,     // Notes
]
function transformRow(rowData) {
  // For each field in the row...
  return rowData.map(function(fieldData, columnIndex, wholeRow) {
    // ...use the appropriate transform function to transform it.
    return columnTransforms[columnIndex](fieldData, wholeRow);
  });
}

function transformTimestamp(input) {
  // convert the timezone to US Eastern Time, and output it as RFC 3339 (which is basically strict ISO 8601)
  return moment(input, 'M/D/YY h:mm:ss A', 'America/Los_Angeles').tz('America/New_York').format();
}

function transformFreeInput(input) {
  // we don't need to do anything to the free input fields
  return input;
}

function transformZIP(input) {
  // pad the ZIP with 0s to make sure it's 5 characters long
  const padding = Array(5 - input.length).fill('0').join('');
  return padding + input;
}

function transformFullName(input) {
  return input.toLocaleUpperCase();
}

function transformDuration(input) {
  // Durations come to us in the format H:MM:SS.MS (where H can have 1+ digits)
  const matches = /^(\d+):(\d{2}):(\d{2}\.\d+)$/.exec(input);
  const hours = +matches[1];
  const minutes = +matches[2];
  const seconds = +matches[3];

  // transform them to unrounded seconds in floating point (60 seconds in a minute; 60 minutes in an hour)
  return (hours * 60 * 60) + (minutes * 60) + seconds;
}

function transformTotalDuration(input, row) {
  // FooDuration is column 4 and BarDuration is column 5 (0-based); we want to get the sum of both
  // Floating-point addition is sometimes fraught (see https://floating-point-gui.de/),
  // so we'll convert them to milliseconds before adding them
  return ((transformDuration(row[4]) * 1000) + (transformDuration(row[5]) * 1000)) / 1000;
}
