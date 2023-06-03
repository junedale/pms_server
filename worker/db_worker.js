/**
 * @typedef {import('worker_threads').MessagePort} MessagePort
 */
/**
 * Represents the worker thread's communication port.
 * @type {MessagePort}
 */
const { parentPort } = require('worker_threads');
/**
 * Represents the database connection pool.
 * @type {*}
 */
const pool = require('../db');


/**
 * Handles messages received by the worker thread.
 * @param {*} message - The message received by the worker thread.
 */
parentPort.on('message', async (message) => {
    try {
        const connection = await pool.getConnection();
        const { type, request, data } = message;    
        const { cluster_id, temperature, humidity } = data;

        if (request === 'insert') {
            handleInsertRequest(connection, type, cluster_id, temperature, humidity);
        }

        releaseConnection(connection);
    } catch (error) {
        postMessageToParent('error', error.message);
    }
});

/**
 * Releases the database connection after performing the necessary operations.
 * @param {*} connection - The database connection.
 */
function releaseConnection(connection) {
    connection.release();
}

/**
 * Handles the 'insert' request by performing the appropriate database operations.
 * @param {*} connection - The database connection.
 * @param {number} type - The type of request.
 * @param {string} cluster_id - The cluster ID.
 * @param {number} temperature - The temperature value.
 * @param {number} humidity - The humidity value.
 */
async function handleInsertRequest(connection, type, cluster_id, temperature, humidity) {
    if (type === 0) {
        const selectQuery = 'SELECT cluster_id FROM cluster_records WHERE cluster_id = ?';
        const [rows] = await connection.query(selectQuery, [cluster_id]);

        if (rows.length === 0) {
            const insertQuery = 'INSERT INTO cluster_records (cluster_id) VALUES (?)';
            await connection.query(insertQuery, [cluster_id]);
        }
    } else {
        const insertQuery = 'INSERT INTO sensor_records (cluster_id, temperature, humidity) VALUES (?, ?, ?)';
        await connection.query(insertQuery, [cluster_id, temperature, humidity]);
    }

    postMessageToParent('success');
}

/**
 * Posts a message to the parent thread with the result of the database operation.
 * @param {string} result - The result of the operation.
 * @param {string|null} [error] - The error message if the operation encountered an error.
 */
function postMessageToParent(result, error = null) {
    parentPort.postMessage({ result, error });
}
