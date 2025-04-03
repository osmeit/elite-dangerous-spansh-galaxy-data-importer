const fs = require('fs');
const zlib = require('zlib');
const { pipeline } = require('stream');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');

const structureAnalysis = {};
let recordCount = 0;
const SAMPLE_SIZE = 20000;  // Anzahl der zu analysierenden Datensätze

function analyzeObject(obj, path = '') {  
  for (const [key, value] of Object.entries(obj)) {
    const fullPath = path ? `${path}.${key}` : key;
    const type = Array.isArray(value) ? 'array' : typeof value;
    
    if (!structureAnalysis[fullPath]) {
      structureAnalysis[fullPath] = {
        types: new Set(),
        example: value,
        count: 0
      };
    }

    structureAnalysis[fullPath].types.add(type);
    structureAnalysis[fullPath].count++;
    
    if (typeof value === 'object' && value !== null) {
      analyzeObject(value, fullPath);
    }
  }
}

function processFile() {
  console.log('Starte Strukturanalyse...');
  const startTime = Date.now();

  const stream = fs.createReadStream('./json/galaxy.json.gz')
    .pipe(zlib.createGunzip())
    .pipe(parser())
    .pipe(streamArray());

  stream.on('data', ({ value }) => {
    if (recordCount++ >= SAMPLE_SIZE) {
      stream.destroy();
      printResults();
      return;
    }
    
    analyzeObject(value);
  });

  stream.on('end', printResults);
  stream.on('error', (err) => console.error('Fehler:', err));

  function printResults() {
    console.log('\nAnalyse abgeschlossen!');
    console.log(`Untersuchte Datensätze: ${recordCount}`);
    console.log('Felderstruktur:\n');
    
    const results = Object.entries(structureAnalysis).map(([field, stats]) => ({
      field,
      types: Array.from(stats.types),
      example: stats.example,
      occurrence: `${((stats.count / recordCount) * 100).toFixed(1)}%`
    }));

    console.table(results);
    console.log(`Dauer: ${(Date.now() - startTime)/1000}s`);
    process.exit();
  }
}

processFile();