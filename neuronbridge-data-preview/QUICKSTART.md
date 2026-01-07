# Quick Start Guide

Get NeuronBridge Data Preview up and running in 5 minutes.

## Prerequisites Check

Before starting, ensure you have:

1. **Node.js installed** (v16+)
   ```bash
   node --version  # Should show v16 or higher
   ```

2. **MongoDB running** with NeuronBridge data
   ```bash
   # Check if MongoDB is running
   mongosh --eval "db.version()"

   # Check if you have the neuronbridge database
   mongosh neuronbridge --eval "db.neuronMetadata.count()"
   ```

3. **Color depth MIP images** on your local filesystem
   - You should know the path to the directory containing your images
   - Images should be organized by alignment space and library

## Installation Steps

### 1. Install Dependencies

```bash
# In the neuronbridge-local directory
npm run install:all
```

This will install all dependencies for the root, server, and client.

### 2. Configure the Application

Copy the example environment file:
```bash
cp .env.example .env
```

Edit the `.env` file in the **server** directory and set these required values:

```env
# Your MongoDB connection
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=neuronbridge

# Path to your color depth MIP images - this is used only to restrict access to data
# located under the IMAGE_BASE_PATH dir hierarchy not to build any path to any data
IMAGE_BASE_PATH=/absolute/path/to/your/images

# Server port (default: 3000)
PORT=3000
```

**Important:** Replace `/absolute/path/to/your/images` with the actual path to your image directory.

**Note:** The `.env` file should be placed in the `server/` directory, not the root directory.

### 3. Start the Application

For a quick start in production mode:

```bash
# Build the frontend
npm run build

# Start the server
npm run start:server
```

You should see:
```
Connected to MongoDB
NeuronBridge Data Preview server running on http://localhost:3000
Environment: development
Database: neuronbridge
Image base path: /your/path/to/images
```

For a quick start in development mode:

```bash
# Build the frontend
npm run build

# Start the server
npm run start:dev
```


### 4. Open Your Browser

Navigate to: `http://localhost:3000`

You should see the NeuronBridge Data Preview interface with your neurons!

## Development Mode (Optional)

If you want to develop/modify the code, use the unified development command:

```bash
npm run start:dev
```

This will start both the backend and frontend in development mode:
- Backend on `http://localhost:3000` (with auto-reload via nodemon)
- Frontend dev server on `http://localhost:3001` (with hot reload and proxy to backend)

Alternatively, you can run them separately:
```bash
# Terminal 1: Start backend with auto-reload
npm run start:server

# Terminal 2: Start frontend dev server
npm run start:client
```

## Troubleshooting

### "Cannot connect to MongoDB"
- Check MongoDB is running: `mongosh`
- Verify database name in `.env` matches your setup
- Check connection string format

### "No neurons appearing"
- Verify data exists: `mongosh neuronbridge --eval "db.neuronMetadata.findOne()"`
- Check MongoDB database name in `.env`
- Review browser console for errors

### "Images not loading"
- Verify `IMAGE_BASE_PATH` in `.env`
- Check file paths in MongoDB match your filesystem
  ```bash
  mongosh neuronbridge --eval "db.neuronMetadata.findOne({}, {'computeFiles': 1})"
  ```
- Ensure the path in `computeFiles.SourceColorDepthImage.name` exists relative to `IMAGE_BASE_PATH`

### Port 3000 already in use
- Change `PORT` in `server/.env` to another value (e.g., 3001)
- If running in development mode, also update the proxy in `client/package.json`
- Or stop the process using port 3000

## Next Steps

Once running, you can:

1. **Browse neurons** - Use the main page to browse all neurons with filters
2. **View matches** - Click on any neuron to see its color depth matches
3. **Search** - Use the search page to find specific neurons by name or ID

## Need More Help?

- See full documentation in `README.md`
- Check server logs for detailed error messages
- Verify your MongoDB schema matches expectations

## Common MongoDB Queries

Check your data:

```bash
# Count total neurons
mongosh neuronbridge --eval "db.neuronMetadata.count()"

# List all libraries
mongosh neuronbridge --eval "db.neuronMetadata.distinct('libraryName')"

# List alignment spaces
mongosh neuronbridge --eval "db.neuronMetadata.distinct('alignmentSpace')"

# View a sample neuron
mongosh neuronbridge --eval "db.neuronMetadata.findOne()"

# Count color depth matches
mongosh neuronbridge --eval "db.cdMatches.count()"

# View a sample match
mongosh neuronbridge --eval "db.cdMatches.findOne()"
```
