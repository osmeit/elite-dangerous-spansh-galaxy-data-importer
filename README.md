# Elite Galaxy Data Importer

The **Elite Galaxy Data Importer** is a tool designed to process and import star system data from [Spansh Dumps](https://www.spansh.co.uk/dumps) into a PostgreSQL database. This project is tailored for the game *Elite Dangerous* and provides a robust solution for handling large datasets, enabling efficient querying and analysis of the galaxy's systems and bodies.

## Features

- **Data Import**: Converts JSON dumps from Spansh into a structured PostgreSQL database.
- **Batch Processing**: Efficiently processes large datasets using batch inserts.
- **Indexing and Optimization**: Automatically creates indexes and materialized views for faster queries.
- **Dockerized Setup**: Runs seamlessly using Docker Compose on Linux.
- **Error Handling**: Logs failed batches for later review and reprocessing.

## Planned Features

- **Change Tracking**: Support for importing and analyzing monthly, weekly, or daily changes in the galaxy data.
- **Cross-Platform Support**: Extend compatibility to other operating systems.
- **Additional Tools**: Add utilities for advanced data analysis and visualization.

## Prerequisites

- Docker and Docker Compose installed on your system.
- A Linux environment (tested only on Linux).

## Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd elite-galaxy-data-importer
```

### 2. Configure Environment Variables

Edit the `.env.processor` file to configure the batch size, file path, and other settings:

```env
FILE_PATH=./json/galaxy_1month.json.gz
BATCH_SIZE=5000
MAX_RETRIES=0
INIT_DB=true
REMOVE_OLD_DB=true
```

### 3. Download the Data

Download the latest JSON dump from [Spansh Dumps](https://www.spansh.co.uk/dumps) and place it in the `json/` directory.

### 4. Start the Application

Run the application using Docker Compose:

```bash
docker-compose up -d
```

This will start the PostgreSQL database and the JSON processor in detached mode.

### 5. View Logs

To monitor the logs of the processor, use the following command:

```bash
docker-compose logs -f processor
```

### 6. Access the Database

Once the import is complete, you can connect to the PostgreSQL database using your preferred client. The default credentials are:

- **Host**: `localhost`
- **Port**: `5432`
- **User**: `admin`
- **Password**: `secret`
- **Database**: `mydb`

### 7. Logs and Failed Batches

- Logs are displayed in the console during processing.
- Failed batches are logged in `failed_batches.log.json` for later review.

## Database Schema

### Systems Table

| Column            | Type          | Description                       |
|--------------------|---------------|-----------------------------------|
| `system_id64`      | BIGINT        | Unique identifier for the system |
| `name`             | TEXT          | Name of the system               |
| `coords_x`         | NUMERIC       | X coordinate                     |
| `coords_y`         | NUMERIC       | Y coordinate                     |
| `coords_z`         | NUMERIC       | Z coordinate                     |
| `allegiance`       | JSONB         | Allegiance data                  |
| `government`       | JSONB         | Government data                  |
| `primary_economy`  | TEXT          | Primary economy                  |
| `secondary_economy`| TEXT          | Secondary economy                |
| `security`         | TEXT          | Security level                   |
| `population`       | BIGINT        | Population count                 |
| `body_count`       | INT           | Number of bodies in the system   |
| `date`             | TIMESTAMP     | Last updated date                |

### Bodies Table

| Column            | Type          | Description                       |
|--------------------|---------------|-----------------------------------|
| `body_id64`        | BIGINT        | Unique identifier for the body   |
| `system_id64`      | BIGINT        | Reference to the system          |
| `body_id`          | INT           | Body ID within the system        |
| `name`             | TEXT          | Name of the body                 |
| `type`             | TEXT          | Type of the body (e.g., Star)    |
| `body_data`        | JSONB         | Detailed body data               |
| `discovered_date`  | TIMESTAMP     | Discovery date                   |

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests for new features, bug fixes, or improvements.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- Data provided by [Spansh Dumps](https://www.spansh.co.uk/dumps).
- Inspired by the *Elite Dangerous* community.

---
Happy exploring, Commander!
