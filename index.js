const async = require('async');
const bedrock = require('bedrock');
const cluster = require('cluster');
const config = bedrock.config;
const database = require('bedrock-mongodb');

require('./config');

config.paths.log = '/tmp';

config.lambda = config.lambda || {ready: false};

let ready = false;
let requestCount = 0;
let workerStarted = false;

bedrock.events.on('bedrock-mongodb.ready', callback => async.auto({
  openCollections: callback => database.openCollections(['lambda'], err => {
    console.log('1111111111', err);
    callback(err);
  })
}, err => callback(err)));

bedrock.events.on('bedrock.started', () => {
  console.log('!!!!!!!!!!!!!!STARTED!!!!!!!!!!!!!!!!!!!!!!', process.pid);
  ready = true;
});

process.on('message', m => {
  console.log('WORER RECEIVED MESSAGE', process.pid, m);
  console.log('READY', process.pid, ready);
  if(m.type === 'lambda.request') {
    executeQuery(m.query);
  }
});

function executeQuery(query) {
  console.log('EXECUTE QUERY', query);
  if(!ready) {
    return setTimeout(() => executeQuery(query), 250);
  }
  database.collections.lambda.find(query).toArray((err, result) => {
    if(err) {
      throw err;
    }
    process.send({
      type: 'lambda.result',
      payload: result
    });
  });
}

exports.handler = function(event, context, callback) {
  context.callbackWaitsForEmptyEventLoop = false;
  // Go through all workers
  function findWorker(callback) {
    const w = Object.keys(cluster.workers);
    if(w.length === 0) {
      return callback();
    }
    callback(cluster.workers[w]);
  }

  function doIt() {
    console.log('MASTER LOOKING FOR WORKER', Date.now());
    findWorker(worker => {
      if(!worker) {
        return setTimeout(doIt, 250);
      }
      console.log('Found worker', worker.id);
      console.log('requestCount for this container:', requestCount);

      // the worker is already warm, so just send the query, no need to wait
      // for the worker to start
      if(workerStarted) {
        console.log('Sending query to warm worker');
        worker.on('message', lambdaResult);
        return worker.send({type: 'lambda.request', query: event});
      }
      worker.on('message', workerStart);

      function lambdaResult(m) {
        if(m.type === 'lambda.result') {
          requestCount++;
          console.log('Calling lambda callback', process.pid, m.payload);
          worker.removeListener('message', lambdaResult);
          callback(null, m.payload);
        }
      }

      function workerStart(m) {
        if(m.type === 'bedrock.worker.started') {
          console.log('WORKER STARTED', m);
          workerStarted = true;
          worker.on('message', lambdaResult);
          worker.send({type: 'lambda.request', query: event});
        }
      }
    });
  }

  doIt();
};

// specifying script here is critical, otherwise it runs AWS launcher code
bedrock.start({script: __filename});
