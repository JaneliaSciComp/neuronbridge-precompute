# NeuronBridge Local

A local version of the NeuronBridge web application that uses local MongoDB data and filesystem instead of AWS services.

## Overview

This application provides a local interface to browse neuron data from the NeuronBridge color depth MIP search database. It's a simplified, integrated version that:

- Connects to your local MongoDB database (populated by neuronbridge-colormipsearch tools)
- Serves neuron images from your local filesystem
- Provides a web interface to browse neurons, view matches, and search

## Prerequisites

- **Node.js** v16 or higher
- **MongoDB** with NeuronBridge data (populated using neuronbridge-colormipsearch tools)
- **Local filesystem** with neuron color depth MIP images

## Installation

### 1. Install Dependencies

```bash
# Install all dependencies (root, server, and client)
npm run install:all
```

### 2. Configure Environment

Create a `.env` file in the root directory (copy from `.env.example`):

```bash
cp .env.example .env
```

Edit `.env` and configure your settings:

```env
# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=neuronbridge
MONGODB_USERNAME=
MONGODB_PASSWORD=
MONGODB_AUTH_DATABASE=admin

# Server Configuration
PORT=3000
NODE_ENV=development

# Image File Paths
IMAGE_BASE_PATH=/path/to/your/colordepth/images

# Pagination
DEFAULT_PAGE_SIZE=50
MAX_PAGE_SIZE=500
```

**Important:** Set `IMAGE_BASE_PATH` to the directory containing your color depth MIP images. The path structure should match the organization used by neuronbridge-colormipsearch tools:
```
IMAGE_BASE_PATH/
├── JRC2018_Unisex_20x_HR/
│   ├── flylight_splitgal4_drivers/
│   │   └── *.png
│   └── flyem_hemibrain/
│       └── *.png
└── JRC2018_VNC_Unisex_40x_DS/
    └── ...
```

## Running the Application

### Development Mode

Run both the backend and frontend in development mode with a single command:

```bash
npm run start:dev
```

This will start:
- Backend server on `http://localhost:3000` (with auto-reload via nodemon)
- Frontend dev server on `http://localhost:3001` (with hot reload and proxy to backend)

Alternatively, you can run them separately:
```bash
# Terminal 1: Start the backend server
npm run start:server

# Terminal 2: Start the frontend dev server
npm run start:client
```

### Production Mode

Build and run the production version:

```bash
# Build the frontend
npm run build

# Start the server
npm run start:server
```

Access the application at `http://localhost:3000`

## Project Structure

```
neuronbridge-local/
├── server/                # Backend server
│   ├── server.js          # Main Express server
│   ├── src/
│   │   ├── config/
│   │   │   └── database.js    # MongoDB connection
│   │   ├── routes/
│   │   │   ├── neurons.js     # Neuron listing & details API
│   │   │   ├── cdmatches.js   # Color depth matches API
│   │   │   ├── search.js      # Search API
│   │   │   └── images.js      # Image serving routes
│   │   └── utils/
│   │       └── serialization.js # BSON serialization utilities
│   └── package.json
├── client/                # React frontend
│   ├── public/
│   ├── src/
│   │   ├── components/
│   │   │   ├── NeuronList.js
│   │   │   ├── NeuronDetail.js
│   │   │   └── Search.js
│   │   ├── App.js
│   │   └── index.js
│   └── package.json
├── package.json           # Root package with unified scripts
├── .env.example
└── README.md
```

## API Endpoints

The backend provides the following REST API:

### Neurons
- `GET /api/neurons` - List neurons with pagination and filters
  - Query params: `page`, `limit`, `libraryName`, `alignmentSpace`, `publishedName`, `mipId`
  - Automatically filters out neurons with `publishedName: "No Consensus"` and tags `junk` or `validationError`
  - Requires `computeFiles.InputColorDepthImage` to exist
- `GET /api/neurons/:id` - Get neuron by ID
- `GET /api/neurons/meta/libraries` - Get all library names
  - Query params: `isEM` (true/false), `alignmentSpace`
- `GET /api/neurons/meta/alignment-spaces` - Get all alignment spaces

### Matches
- `GET /api/cdmatches/em-cdms/:neuronId` - Get color depth matches for an EM neuron (EM→LM)
  - Query params: `page`, `limit`, `minScore`, `minMatchingPixels`, `withGradScore`, `targetLibrary`
  - Searches by `maskImageRefId` (EM neurons as masks)
  - Returns enriched matches with full neuron metadata from lookup
  - Includes `negativeScore` calculation for matches with `gradientAreaGap`
  - Sorted by `normalizedScore` (descending)
- `GET /api/cdmatches/lm-cdms/:neuronId` - Get color depth matches for an LM neuron (LM→EM)
  - Query params: `page`, `limit`, `minScore`, `minMatchingPixels`, `withGradScore`, `targetLibrary`
  - Searches by `matchedImageRefId` (LM neurons as matched images)
  - Returns enriched matches with full neuron metadata from lookup
  - Includes `negativeScore` calculation for matches with `gradientAreaGap`
  - Sorted by `normalizedScore` (descending)

### Search
- `POST /api/search` - Search neurons with complex criteria
  - Body params: `publishedName` (string/regex), `libraryName` (string or array), `alignmentSpace`, `neuronTerms` (array), `mipId`, `sourceRefId`, `page`, `limit`
  - Returns matching neurons with pagination
- `GET /api/search/autocomplete` - Autocomplete suggestions
  - Query params: `q` (query string), `field` (publishedName/mipId/sourceRefId, default: publishedName), `limit` (default: 10, max: 50)
  - Returns array of `{id, value}` objects

### Images
- `GET /api/images/thumbnail` - Generate and serve thumbnail images
  - Query params: `imagePath` (absolute path to image)
  - Supports PNG, TIF/TIFF (generates thumbnail), JPG/JPEG (serves original)
  - Returns resized thumbnails (default: 200x200, preserving aspect ratio)

### Health
- `GET /api/health` - Health check endpoint
  - Returns `{status: 'ok', timestamp: ISO8601}`

## MongoDB Collections

The application expects the following MongoDB collections (created by neuronbridge-colormipsearch):

### `neuronMetadata`
Stores neuron entities (both LM and EM):
- `_id` - Unique identifier
- `mipId` - MIP ID from JACS
- `publishedName` - Published neuron name
- `libraryName` - Library name
- `alignmentSpace` - Alignment space (e.g., JRC2018_Unisex_20x_HR)
- `sourceRefId` - Source reference ID
- `computeFiles` - Map of file references (SourceColorDepthImage, etc.)
- `neuronTerms` - Array of anatomical terms

### `cdMatches`
Color depth match results:
- `maskImageRefId` - ID of the mask neuron (typically EM)
- `matchedImageRefId` - ID of the matched neuron (typically LM)
- `normalizedScore` - Match score
- `matchingPixels` - Number of matching pixels
- `gradientAreaGap` - Gradient-based score

## Features

### Browse Neurons
- Paginated list of all neurons
- Filter by library, alignment space, and published name
- View neuron images directly from local filesystem

### Neuron Details
- View full neuron metadata
- Display color depth MIP images
- Show color depth matches for EM neurons (EM→LM matches)
- Show color depth matches for LM neurons (LM→EM matches)
- Navigate between related neurons

### Search
- Search by published name, MIP ID, or source reference ID
- Full-text search support
- Autocomplete suggestions

## Troubleshooting

### Images not loading
1. Check `IMAGE_BASE_PATH` in `.env` is correct
2. Verify file paths in MongoDB match your filesystem structure
3. Check file permissions
4. Look at browser console and server logs for errors

### MongoDB connection fails
1. Ensure MongoDB is running: `mongod --version`
2. Check connection string in `.env`
3. Verify database name matches your setup
4. Check authentication credentials if using auth

### No data appearing
1. Verify MongoDB has data: `mongo neuronbridge --eval "db.neuronMetadata.count()"`
2. Check that collections exist and have the expected schema
3. Review server logs for database query errors

## Development

### Adding new API endpoints
1. Create route handler in `server/src/routes/`
2. Register route in `server/server.js`
3. Update this README

### Modifying the frontend
- React components are in `client/src/components/`
- Styles are in `client/src/App.css`
- Run `npm run start:dev` from the root directory for hot reload of both frontend and backend

### Available NPM Scripts

**Root level:**
- `npm run install:all` - Install all dependencies (root, server, and client)
- `npm run build` - Build the client for production
- `npm run start:server` - Start the server in production mode
- `npm run start:client` - Start the client dev server
- `npm run start:dev` - Start both server and client in development mode (concurrently)

**Server level (from `server/` directory):**
- `npm start` - Start server in production mode
- `npm run start:dev` - Start server in development mode with nodemon

**Client level (from `client/` directory):**
- `npm start` - Start dev server with hot reload
- `npm run build` - Build for production

## Differences from Production NeuronBridge

This local version is simplified and does not include:
- User authentication/authorization
- Custom image uploads and alignment
- AWS S3 integration
- Cognito/OAuth integration
- GraphQL/AppSync
- Search result caching
- Advanced analytics
- Volume viewer integration
- ZIP downloads of match results

## License

BSD 3-Clause License (same as neuronbridge-web and colormipsearch)

## Related Repositories

- [neuronbridge-web](https://github.com/JaneliaSciComp/neuronbridge) - Production web application
- [neuronbridge-colormipsearch](https://github.com/JaneliaSciComp/colormipsearch) - Color depth MIP search tools and data processing

## Support

For issues with:
- **This local application**: Check server logs and browser console
- **Data generation**: See neuronbridge-colormipsearch documentation
- **Production NeuronBridge**: Visit https://neuronbridge.janelia.org
