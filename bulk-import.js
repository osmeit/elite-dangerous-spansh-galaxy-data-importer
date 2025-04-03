const { Pool } = require('pg');
const zlib = require('zlib');
const { pipeline } = require('stream');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const fs = require('fs');
const { Transform } = require('stream');

// Config
const BATCH_SIZE = process.env.BATCH_SIZE || 5000; // Erhöhe die Batch-Größe
const MAX_RETRIES = process.env.MAX_RETRIES || 0;
const INIT_DB = process.env.INIT_DB || true;
const REMOVE_OLD_DB = process.env.INIT_DB || true;
const FILE_PATH = process.env.FILE_PATH;

if (!FILE_PATH) {
    console.error('Error: No file path to "galaxy.json.gz" provided in FILE_PATH environment variable');
    process.exit(1);
}

class BatchTransformer extends Transform {
    constructor() {
        super({ objectMode: true });
        this.batch = [];
    }

    _transform(chunk, encoding, callback) {
        this.batch.push(chunk.value); // Nur das value-Objekt speichern
        if (this.batch.length >= BATCH_SIZE) {
            this.push(this.batch);
            this.batch = [];
        }
        callback();
    }

    _flush(callback) {
        if (this.batch.length > 0) {
            this.push(this.batch);
        }
        callback();
    }
}

// PostgreSQL Connection mit optimierten Einstellungen
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
        console.log('Erstelle Datenbanktabellen...');

        if(REMOVE_OLD_DB) await client.query(`
            DROP TABLE IF EXISTS bodies;
            DROP TABLE IF EXISTS systems;
            DROP MATERIALIZED VIEW IF EXISTS system_stats;
        `);

        await client.query(`     
        CREATE TABLE systems (
          system_id64 BIGINT PRIMARY KEY,
          name TEXT NOT NULL,
          coords_x NUMERIC NOT NULL,
          coords_y NUMERIC NOT NULL,
          coords_z NUMERIC NOT NULL,
          allegiance JSONB,
          government JSONB,
          primary_economy TEXT,
          secondary_economy TEXT,
          security TEXT,
          population BIGINT,
          body_count INT,
          date TIMESTAMP WITH TIME ZONE NOT NULL
        );
  
        CREATE TABLE bodies (
          body_id64 BIGINT PRIMARY KEY,
          system_id64 BIGINT REFERENCES systems(system_id64),
          body_id INT NOT NULL,
          name TEXT NOT NULL,
          type TEXT NOT NULL,
          body_data JSONB NOT NULL,
          discovered_date TIMESTAMP WITH TIME ZONE NOT NULL
        );
      `);

        console.log('Tabellen erfolgreich erstellt');
    } finally {
        client.release();
    }
}

const copyToDatabase = async (tableName, columns, data) => {
    const client = await pool.connect();
    try {
        const tempFile = `/tmp/${tableName}_batch_${Date.now()}.csv`;
        const csvData = data.map(row => columns.map(col => row[col]).join(',')).join('\n');
        fs.writeFileSync(tempFile, csvData);

        const copyQuery = `
            COPY ${tableName} (${columns.join(', ')})
            FROM '${tempFile}'
            WITH (FORMAT csv)
        `;
        await client.query(copyQuery);
        fs.unlinkSync(tempFile); // Lösche die temporäre Datei
    } finally {
        client.release();
    }
};

async function insertBatch(batch, retryCount = 0) {
    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // Systeme Batch Insert
        const systemsValues = batch.map(sys =>
            [
                sys.id64,
                sys.name,
                sys.coords.x,
                sys.coords.y,
                sys.coords.z,
                sys.allegiance ? JSON.stringify(sys.allegiance) : null,
                sys.government ? JSON.stringify(sys.government) : null,
                sys.primaryEconomy,
                sys.secondaryEconomy,
                sys.security,
                sys.population,
                sys.bodyCount,
                sys.date
            ]);


        await client.query(`
      INSERT INTO systems (
        system_id64, name, coords_x, coords_y, coords_z, 
        allegiance, government, primary_economy, 
        secondary_economy, security, population, 
        body_count, date
      ) SELECT * FROM UNNEST(
        $1::bigint[],
        $2::text[],
        $3::numeric[],
        $4::numeric[],
        $5::numeric[],
        $6::jsonb[],
        $7::jsonb[],
        $8::text[],
        $9::text[],
        $10::text[],
        $11::bigint[],
        $12::int[],
        $13::timestamptz[]
      ) 
      ON CONFLICT (system_id64) DO NOTHING
    `, systemsValues.reduce((acc, val) => {
            val.forEach((v, i) => acc[i].push(v));
            return acc;
        }, Array(13).fill().map(() => [])));

        // Körper Batch Insert
        const bodies = batch.flatMap(sys =>
            (sys.bodies || []).map(body => ({
                system_id64: sys.id64,
                body_id64: body.id64,
                body_id: body.bodyId,
                name: body.name,
                type: body.type,
                body_data: {
                    subType: body.subType,
                    distanceToArrival: body.distanceToArrival,
                    orbitalParams: {
                        period: body.orbitalPeriod,
                        eccentricity: body.orbitalEccentricity
                    }
                },
                update_time: body.updateTime
            })));

        if (bodies.length > 0) {
            const bodiesValues = bodies.map(b => [
                b.body_id64,
                b.system_id64,
                b.body_id,
                b.name,
                b.type,
                b.body_data,
                b.update_time
            ]);

            await client.query(`
        INSERT INTO bodies (
          body_id64, system_id64, body_id, name, 
          type, body_data, discovered_date
        ) SELECT * FROM UNNEST(
          $1::bigint[],
          $2::bigint[],
          $3::int[],
          $4::text[],
          $5::text[],
          $6::jsonb[],
          $7::timestamptz[]
        )
        ON CONFLICT (body_id64) DO NOTHING
      `, bodiesValues.reduce((acc, val) => {
                val.forEach((v, i) => acc[i].push(v));
                return acc;
            }, Array(7).fill().map(() => [])));
        }

        await client.query('COMMIT');
        return true;
    } catch (err) {
        await client.query('ROLLBACK');

        if (retryCount < MAX_RETRIES) {
            console.log(`Retrying batch (${retryCount + 1}/${MAX_RETRIES})`);
            return insertBatch(batch, retryCount + 1);
        }

        console.error('Batch insert failed after retries:', err);
        fs.appendFileSync('failed_batches.log', JSON.stringify(batch) + '\n');
        return false;
    } finally {
        client.release();
    }
}

async function createIndexes() {
    // Verwende einen separaten Client außerhalb des Connection-Pools
    const { Client } = require('pg');
    const client = new Client({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT
    });
  
    try {
      await client.connect();
      console.log('Erstelle Indexe...');
  
      // Indexe einzeln außerhalb von Transaktionen erstellen
      await client.query(`
        CREATE INDEX CONCURRENTLY idx_systems_coords 
        ON systems USING GIST (point(coords_x, coords_y, coords_z));
      `);
  
      await client.query(`
        CREATE INDEX CONCURRENTLY idx_bodies_type 
        ON bodies(type);
      `);
  
      await client.query(`
        CREATE INDEX CONCURRENTLY idx_body_data_parents 
        ON bodies USING GIN ((body_data -> 'parents'));
      `);
  
      // Materialized View erstellen
      await client.query(`
        CREATE MATERIALIZED VIEW system_stats AS
        SELECT 
          s.system_id64,
          s.name,
          COUNT(b.body_id64) AS total_bodies,
          SUM((b.body_data->>'solarMasses')::NUMERIC) AS total_mass
        FROM systems s
        JOIN bodies b ON s.system_id64 = b.system_id64
        GROUP BY s.system_id64, s.name;
      `);
  
      // Index für Materialized View
      await client.query(`
        CREATE INDEX CONCURRENTLY idx_system_stats_mass 
        ON system_stats (total_mass);
      `);
  
    } finally {
      await client.end();
    }
  }

async function processGalaxy() {

    if(INIT_DB) await initializeDatabase();

    console.log('Starte Galaxy-Import...');
    const start = Date.now();
    let processed = 0;

    const queue = [];
    let isProcessing = false;

    const MAX_PARALLEL_BATCHES = 5; // Anzahl paralleler Batches

    const processQueue = async () => {
        if (isProcessing || queue.length === 0) return;
        isProcessing = true;

        while (queue.length > 0) {
            const batches = queue.splice(0, MAX_PARALLEL_BATCHES);
            await Promise.all(batches.map(batch => insertBatch(batch)));
            processed += batches.reduce((sum, batch) => sum + batch.length, 0);

            if (processed % (BATCH_SIZE * 10) === 0) {
                const elapsed = (Date.now() - start) / 1000;
                console.log(
                    `Progress: ${processed} systems | ` +
                    `${(processed / elapsed).toFixed(2)} systems/sec | ` +
                    `Queue: ${queue.length} batches`
                );
            }
        }

        isProcessing = false;
    };

    return new Promise((resolve, reject) => {
        pipeline(
            fs.createReadStream(FILE_PATH),
            zlib.createGunzip(),
            parser(),
            streamArray(),
            new BatchTransformer(),
            new Transform({
                objectMode: true,
                transform(batch, encoding, callback) {
                    queue.push(batch);
                    processQueue();
                    callback();
                }
            }),
            (err) => {
                if (err) {
                    console.error('Pipeline error:', err);
                    reject(err);
                } else {
                    // Warte auf verbleibende Batches
                    const check = () => {
                        if (queue.length === 0) resolve();
                        else setTimeout(check, 1000);
                    };
                    check();
                }
            }
        )
    }).then(async () => {
        await createIndexes();
        pool.end();
        resolve();
    })
    .then(() => {
        console.log(`Import abgeschlossen! Systems: ${processed} Dauer: ${Math.round((Date.now() - start) / 1000)}s`);
        pool.end();
    });
}


processGalaxy().catch(console.error);