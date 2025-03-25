const cluster = require("cluster");

if (cluster.isMaster) {
  const numCPUs = 5; // change this value to leverage more threads
  console.log(`Master process ${process.pid} is running`);

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

} else {
  require("./connections-server");
}