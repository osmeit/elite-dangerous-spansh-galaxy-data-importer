const { Pool } = require('pg');
const zlib = require('zlib');
const { pipeline } = require('stream');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const fs = require('fs');
const { Transform } = require('stream');

// Config
const BATCH_SIZE = process.env.BATCH_SIZE || 5000;
const FILE_PATH = process.env.FILE_PATH;

if (!FILE_PATH) {
    console.error('Error: No file path to "galaxy.json.gz" provided in FILE_PATH environment variable');
    process.exit(1);
}

// PostgreSQL Connection
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

async function initializeDatabase() {
    const client = await pool.connect();
    try {
        console.log('Erstelle neue Tabelle...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS public.systems_jsonb (
                id64 BIGINT NOT NULL,
                coords_geom GEOMETRY(PointZ),
                data JSONB,
                CONSTRAINT systems_jsonb_pkey PRIMARY KEY (id64)
            );
        `);
        console.log('Tabelle erfolgreich erstellt');
    } finally {
        client.release();
    }
}

async function insertBatch(batch) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const values = batch.map(sys => [
            sys.id64,
            `POINTZ(${sys.coords.x} ${sys.coords.y} ${sys.coords.z})`,
            JSON.stringify(sys)
        ]);

        await client.query(`
            INSERT INTO public.systems_jsonb (id64, coords_geom, data)
            SELECT * FROM UNNEST(
                $1::bigint[],
                $2::geometry[],
                $3::jsonb[]
            )
            ON CONFLICT (id64) DO UPDATE SET
                coords_geom = EXCLUDED.coords_geom,
                data = EXCLUDED.data;
        `, values.reduce((acc, val) => {
            val.forEach((v, i) => acc[i].push(v));
            return acc;
        }, Array(3).fill().map(() => [])));

        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Batch insert failed:', err);
        fs.appendFileSync('failed_batches.log', JSON.stringify(batch) + '\n');
    } finally {
        client.release();
    }
}

async function processGalaxy() {
    await initializeDatabase();

    console.log('Starte Galaxy-Import...');
    const start = Date.now();
    let processed = 0;

    return new Promise((resolve, reject) => {
        pipeline(
            fs.createReadStream(FILE_PATH),
            zlib.createGunzip(),
            parser(),
            streamArray(),
            new Transform({
                objectMode: true,
                transform(chunk, encoding, callback) {
                    const batch = [chunk.value];
                    insertBatch(batch).then(() => {
                        processed += batch.length;
                        if (processed % BATCH_SIZE === 0) {
                            const elapsed = (Date.now() - start) / 1000;
                            console.log(
                                `Progress: ${processed} systems | ` +
                                `${(processed / elapsed).toFixed(2)} systems/sec`
                            );
                        }
                        callback();
                    }).catch(callback);
                }
            }),
            (err) => {
                if (err) {
                    console.error('Pipeline error:', err);
                    reject(err);
                } else {
                    console.log(`Import abgeschlossen! Systems: ${processed} Dauer: ${Math.round((Date.now() - start) / 1000)}s`);
                    resolve();
                }
            }
        );
    }).finally(() => pool.end());
}

processGalaxy().catch(console.error);