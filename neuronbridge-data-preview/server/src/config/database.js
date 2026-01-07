const { MongoClient } = require('mongodb');

let db = null;
let client = null;

async function connectDB() {
  if (db) {
    return db;
  }

  const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
  const dbName = process.env.MONGODB_DATABASE || 'neuronbridge';

  const options = {
    maxPoolSize: 10,
    minPoolSize: 5,
  };

  // Add authentication if credentials are provided
  if (process.env.MONGODB_USERNAME && process.env.MONGODB_PASSWORD) {
    options.auth = {
      username: process.env.MONGODB_USERNAME,
      password: process.env.MONGODB_PASSWORD,
    };
    if (process.env.MONGODB_AUTH_DATABASE) {
      options.authSource = process.env.MONGODB_AUTH_DATABASE;
    }
  }

  try {
    client = new MongoClient(uri, options);
    await client.connect();
    db = client.db(dbName);
    console.log(`MongoDB connected: ${dbName}`);
    return db;
  } catch (error) {
    console.error('MongoDB connection error:', error);
    throw error;
  }
}

function getDB() {
  if (!db) {
    throw new Error('Database not initialized. Call connectDB first.');
  }
  return db;
}

async function closeDB() {
  if (client) {
    await client.close();
    db = null;
    client = null;
  }
}

module.exports = {
  connectDB,
  getDB,
  closeDB
};
