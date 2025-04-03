const { Pool } = require('pg');
const zlib = require('zlib');
const { pipeline } = require('stream');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const fs = require('fs');

// PostgreSQL Connection
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  max: 20, // Connection-Pool Größe
  idleTimeoutMillis: 30000
});

// Batch-Verarbeitung für effiziente Inserts
async function insertSystem(system) {
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');

    // System einfügen
    const systemQuery = `
      INSERT INTO systems (
        system_id64, name, coords_x, coords_y, coords_z, 
        allegiance, government, primary_economy, 
        secondary_economy, security, population, 
        body_count, date
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      ON CONFLICT (system_id64) DO NOTHING
    `;
    
    await client.query(systemQuery, [
      system.id64,
      system.name,
      system.coords.x,
      system.coords.y,
      system.coords.z,
      system.allegiance,
      system.government,
      system.primaryEconomy,
      system.secondaryEconomy,
      system.security,
      system.population,
      system.bodyCount,
      system.date
    ]);

    // Körper einfügen
    if (system.bodies && system.bodies.length > 0) {
      const bodyQuery = `
        INSERT INTO bodies (
          body_id64, system_id64, body_id, name, 
          type, body_data, discovered_date
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (body_id64) DO NOTHING
      `;

      for (const body of system.bodies) {
        const bodyData = {
          subType: body.subType,
          distanceToArrival: body.distanceToArrival,
          age: body.age,
          spectralClass: body.spectralClass,
          solarMasses: body.solarMasses,
          orbitalParameters: {
            period: body.orbitalPeriod,
            eccentricity: body.orbitalEccentricity
          },
          parents: body.parents?.map(p => p.Null),
          timestamps: body.timestamps
        };

        await client.query(bodyQuery, [
          body.id64,
          system.id64,
          body.bodyId,
          body.name,
          body.type,
          bodyData,
          body.updateTime
        ]);
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error inserting system:', system.id64, err);
  } finally {
    client.release();
  }
}

// Hauptverarbeitungsfunktion
async function processData() {
  console.log('Starte Datenimport...');
  const start = Date.now();
  let count = 0;

  const stream = fs.createReadStream('./json/galaxy_1month.json.gz')
    .pipe(zlib.createGunzip())
    .pipe(parser())
    .pipe(streamArray());

  stream.on('data', async ({ value }) => {
    stream.pause();
    try {
      await insertSystem(value);
      count++;
      
      if (count % 1000 === 0) {
        console.log(`Verarbeitete Systeme: ${count} (${Math.round((Date.now() - start)/1000)}s)`);
      }
    } catch (err) {
      console.error('Verarbeitungsfehler:', err);
    }
    stream.resume();
  });

  stream.on('end', () => {
    console.log(`Import abgeschlossen! Systeme: ${count} Dauer: ${Math.round((Date.now() - start)/1000)}s`);
    pool.end();
  });
}

processData().catch(console.error);