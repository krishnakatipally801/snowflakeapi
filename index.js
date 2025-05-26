const express = require('express');
const snowflake = require('snowflake-sdk');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Connect to Snowflake
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA
});

// Connect once when app starts
connection.connect((err) => {
  if (err) {
    console.error('Connection failed:', err.message);
  } else {
    console.log('Connected to Snowflake');
  }
});

app.get('/data', (req, res) => {
  connection.execute({
    sqlText: 'SELECT CURRENT_DATE, CURRENT_VERSION()',
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Failed to execute statement:', err.message);
        return res.status(500).json({ error: err.message });
      }
      res.json({ data: rows });
    }
  });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
