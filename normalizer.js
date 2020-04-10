const moment = require('moment-timezone');

const parse = require('csv-parse');
const transform = require('stream-transform');
const stringify = require('csv-stringify');

const parser = parse({ info: true });

const transformer = transform(function(data) {
  let { record, info } = data;
  if(info.records === 0) {
    // it's the header row
    return record;
  } else {
    return transformRow(record);
  }
});

const stringifier = stringify();

parser.on('error', function(err) {
  console.error(err);
});

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
  return rowData.map(function(el, ix, arr) {
    return columnTransforms[ix](el, arr);
  });
}

function transformTimestamp(input) {
  return moment(input, 'YY-M-D h:mm:ss A', 'America/Los_Angeles').tz('America/New_York').format();
}

function transformFreeInput(input) {
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
  // Durations come to us in the format HH:MM:SS.MS
  const matches = /^(\d+):(\d{2}):([\d.]+)$/.exec(input);
  const hours = +matches[1];
  const minutes = +matches[2];
  const seconds = +matches[3];

  // transform them to unrounded seconds in floating point (60 seconds in a minute; 60 minutes in an hour)
  return (hours * 60 * 60) + (minutes * 60) + seconds;
}

function transformTotalDuration(input, row) {
  // FooDuration is column 4 and BarDuration is column 5 (0-based)
  // we want them added together
  return transformDuration(row[4]) + transformDuration(row[5]);
}
