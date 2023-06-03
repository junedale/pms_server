/**
 * @typedef {import('ws').WebSocket} WebSocket
 * @typedef {import('worker_threads').Worker} Worker
 */
const WebSocket = require('ws');
const { Worker } = require('worker_threads');

const workerPool = new Set();
const workerPoolSize = 3;
const requestQueue = [];

/**
 * Creates a pool of worker threads.
 */
function createWorkerPool() {
    for (let i = 0; i < workerPoolSize; i++) {
        const worker = new Worker('./worker/db_worker.js');
        workerPool.add(worker);
    }
}

/**
 * Handles messages received from a worker thread.
 * @param {Worker} worker - The worker thread.
 */
function handleWorkerMessages(worker) {
    worker.on('message', message => {
        worker.busy = false;
        console.log('Worker thread completed:', message);
        if (requestQueue.length > 0) {
            const { type, request, data } = requestQueue.shift();
            assignWorkToWorker(type, request, data);
        }
    });
}

/**
 * Finds an available worker thread from the worker pool.
 * @returns {Worker|null} - The available worker thread, or null if none are available.
 */
function finAvailableWorker() {
    return [...workerPool].find(worker => !worker.busy);
}

/**
 * Assigns work to an available worker thread.
 * @param {string} type - The type of message that was sent.
 * @param {string} request - The request associated with the work i.e insert|update|delete.
 * @param {Object} data - The data required for the work.
 */
function assignWorkToWorker(type, request, data) {
    const worker = finAvailableWorker();
    if (worker) {
        worker.postMessage({ type, request, data });
        worker.busy = true;
    } else {
        requestQueue.push({ type, request, data });
    }
}

/**
 * Handles a socket connection.
 * @param {WebSocket} clientWebSocket - The WebSocket connection.
 */
function handleSocketConnection(clientWebSocket) {
    let isAlive = true;
    const heartbeat = setInterval(() => {
        if (!isAlive) {
            clearInterval(heartbeat);
            clientWebSocket.terminate();
            cleanupClient(clientWebSocket);
        } else {
            isAlive = false;
            clientWebSocket.ping(null, false);
        }
    }, 30000);

    clientWebSocket.on('pong', () => {
        isAlive = true;
    });

    clientWebSocket.on('message', message => {
        try {
            const { message_type, channel, request, data } = JSON.parse(message);

            if (channels.has(channel)) {
                const channelSubscribers = channels.get(channel);
                if (!channelSubscribers.includes(clientWebSocket)) {
                    channelSubscribers.push(clientWebSocket);
                    clientWebSocket.send('You are connected to the channel ' + channel);
                }

                if (channel === 'backend') {
                    assignWorkToWorker(message_type, request, data);
                }
            }

        } catch (error) {
            console.log(error);
        }
    });

    clientWebSocket.on('close', () => {
        cleanupClient(clientWebSocket);
    });
}

/**
 * Cleans up a client WebSocket connection.
 * @param {WebSocket} clientWebSocket - The WebSocket connection to be cleaned up.
 */
function cleanupClient(clientWebSocket) {
    for (const subscribers of channels.values()) {
        const index = subscribers.indexOf(clientWebSocket);
        if (index !== -1) {
            subscribers.splice(index, 1);
            break;
        }
    }
}

/**
 * Represents the WebSocket server.
 * @type {WebSocket.Server}
 */
const wss = new WebSocket.Server({ port: 3001 });

/**
 * Represents the channels and their subscribers.
 * @type {Map<string, WebSocket[]>}
 */
const channels = new Map();
channels.set('backend', []);
channels.set('frontend', []);

/**
 * Event listener for new socket connections.
 * @param {WebSocket} clientWebSocket - The WebSocket connection.
 */
wss.on('connection', handleSocketConnection);

// Initialization
createWorkerPool();
workerPool.forEach(handleWorkerMessages);



