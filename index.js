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

// Connect once on startup
connection.connect((err) => {
  if (err) {
    console.error('Connection failed:', err.message);
  } else {
    console.log('Connected to Snowflake');
  }
});

// Route: Fetch user by ID

app.get('/user-by-ssn', (req, res) => {
  const ssnHash = req.query.SSN_HASH;

  if (!ssnHash) {
    return res.status(400).json({ error: 'Missing ssnHash parameter' });
  }

  const sql = `
    SELECT USERID, DATE, MESSAGE
    FROM ${process.env.SNOWFLAKE_DATABASE}.${process.env.SNOWFLAKE_SCHEMA}.testnotes
    WHERE SSN_HASH = ?
  `;

  connection.execute({
    sqlText: sql,
    binds: [ssnHash],
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Error executing query:', err.message);
        return res.status(500).json({ error: 'Database query failed' });
      }

      if (rows.length === 0) {
        return res.status(404).json({ error: 'User not found' });
      }

      res.json(rows[0]);
    }
  });
});


// Health check route
app.get('/', (req, res) => {
  res.send('Snowflake middleware is running.');
});

// Start server
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
