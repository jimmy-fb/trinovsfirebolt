const dotenv = require("dotenv");
const cluster = require("cluster");

dotenv.config({ path: "../../config/credentials/k6config.env" });

if (cluster.isMaster) {
  const numCPUs = process.env.numCPUs; // change this value to leverage more threads
  console.log(`Master process ${process.pid} is running`);

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

} else {
  require("./connections-server");
}