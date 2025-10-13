const { Pool } = require("pg");

const pool = new Pool({
  user: "master_user",
  host: "localhost",
  database: "aerofusion_db",
  password: "Kartikey@123",
  port: 5432,
});

pool.query("SELECT NOW()", (err, res) => {
  console.log(err, res);
  pool.end();
});