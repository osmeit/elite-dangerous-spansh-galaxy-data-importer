const { Pool } = require('pg');
const zlib = require('zlib');
const { pipeline } = require('stream');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const fs = require('fs');
const { Transform } = require('stream');

// Config
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || 5000, 10);
const FILE_PATH = process.env.FILE_PATH;
const PARALLEL_BATCHES = parseInt(process.env.PARALLEL_BATCHES || 1, 10);

if (!FILE_PATH) {
    console.error('Error: No file path to "galaxy.json.gz" provided in FILE_PATH environment variable');
    process.exit(1);
}

// PostgreSQL Connection Pool
const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    max: 100,
    idleTimeoutMillis: 0,
    connectionTimeoutMillis: 0,
    application_name: 'bulk_importer'
});

// Initialize the database
async function initializeDatabase() {
    const client = await pool.connect();
    try {
        console.log('Initializing database...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS public.systems_jsonb (
                id64 BIGINT NOT NULL,
                coords_geom GEOMETRY(PointZ),
                data JSONB,
                CONSTRAINT systems_jsonb_pkey PRIMARY KEY (id64)
            );
        `);
        console.log('Database initialized successfully.');
    } finally {
        client.release();
    }
}

// Insert a batch of systems into the database
async function insertBatch(batch) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const id64Array = [];
        const coordsArray = [];
        const dataArray = [];

        batch.forEach(sys => {
            if (
                sys.coords &&
                typeof sys.coords.x === 'number' &&
                typeof sys.coords.y === 'number' &&
                typeof sys.coords.z === 'number'
            ) {
                id64Array.push(sys.id64);
                coordsArray.push(`POINTZ(${sys.coords.x} ${sys.coords.y} ${sys.coords.z})`);
                dataArray.push(sys);
            } else {
                console.warn(`Skipping system with invalid coordinates: id64=${sys.id64}`);
            }
        });

        if (id64Array.length > 0) {
            await client.query(`
                INSERT INTO public.systems_jsonb (id64, coords_geom, data)
                SELECT * FROM UNNEST(
                    $1::bigint[],
                    $2::text[],
                    $3::jsonb[]
                ) AS t(id64, coords_text, data)
                ON CONFLICT (id64) DO UPDATE SET                    
                    data = EXCLUDED.data
                RETURNING id64, coords_text, data
            `, [id64Array, coordsArray, dataArray]);
        }

        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Batch insert failed:', err);
        fs.appendFileSync('failed_batches.log', JSON.stringify(batch) + '\n');
    } finally {
        client.release();
    }
}

// Process the JSON file in batches
async function processGalaxy() {
    await initializeDatabase();

    console.log('Starting galaxy import...');
    const start = Date.now();
    let processed = 0;
    let batchQueue = [];

    return new Promise((resolve, reject) => {
        pipeline(
            fs.createReadStream(FILE_PATH),
            zlib.createGunzip(),
            parser(),
            streamArray(),
            new Transform({
                objectMode: true,
                transform(chunk, encoding, callback) {
                    batchQueue.push(chunk.value);

                    if (batchQueue.length >= BATCH_SIZE) {
                        const batch = batchQueue;
                        batchQueue = [];

                        insertBatch(batch).then(() => {
                            processed += batch.length;
                            const elapsed = (Date.now() - start) / 1000;
                            console.log(
                                `Progress: ${processed} systems | ` +
                                `${(processed / elapsed).toFixed(2)} systems/sec`
                            );
                            callback();
                        }).catch(callback);
                    } else {
                        callback();
                    }
                }
            }),
            (err) => {
                if (err) {
                    console.error('Pipeline error:', err);
                    reject(err);
                } else {
                    if (batchQueue.length > 0) {
                        insertBatch(batchQueue).then(() => {
                            const elapsed = (Date.now() - start) / 1000;
                            console.log(
                                `Import completed! Systems: ${processed} | Duration: ${Math.round(elapsed)}s`
                            );
                            resolve();
                        }).catch(reject);
                    } else {
                        const elapsed = (Date.now() - start) / 1000;
                        console.log(
                            `Import completed! Systems: ${processed} | Duration: ${Math.round(elapsed)}s`
                        );
                        resolve();
                    }
                }
            }
        );
    }).finally(() => pool.end());
}

// Start the import process
processGalaxy().catch(console.error);