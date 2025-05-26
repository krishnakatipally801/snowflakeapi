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
app.get('/user/:id', (req, res) => {
  const userId = parseInt(req.params.id);

  if (isNaN(userId)) {
    return res.status(400).json({ error: 'Invalid user ID' });
  }

  const sql = `
    SELECT name, city, state, email
    FROM ${process.env.SNOWFLAKE_DATABASE}.${process.env.SNOWFLAKE_SCHEMA}.users
    WHERE id = ?
  `;

  connection.execute({
    sqlText: sql,
    binds: [userId],
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
