const express = require('express');
const router = express.Router();
const { getDB } = require('../config/database');
const { convertStringToId, serializeDocument, serializeDocuments } = require('../utils/serialization');

const DEFAULT_PAGE_SIZE = parseInt(process.env.DEFAULT_PAGE_SIZE) || 50;
const MAX_PAGE_SIZE = parseInt(process.env.MAX_PAGE_SIZE) || 500;

/**
 * GET /api/neurons
 * List all neurons with pagination and filtering
 * Query params:
 *   - page: page number (default: 0)
 *   - limit: items per page (default: 50, max: 500)
 *   - libraryName: filter by library
 *   - alignmentSpace: filter by alignment space
 *   - publishedName: filter by published name (partial match)
 */
router.get('/', async (req, res) => {
  try {
    const db = getDB();
    const collection = db.collection('neuronMetadata');

    const page = parseInt(req.query.page) || 0;
    const limit = Math.min(parseInt(req.query.limit) || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
    const skip = page * limit;

    // Build filter
    const filter = {};
    if (req.query.libraryName) {
      filter.libraryName = req.query.libraryName;
    }
    if (req.query.alignmentSpace) {
      filter.alignmentSpace = req.query.alignmentSpace;
    }
    filter.publishedName = { $ne: 'No Consensus' };
    if (req.query.publishedName) {
      filter.publishedName = { $regex: req.query.publishedName, $options: 'i' };
    }
    if (req.query.mipId) {
      filter.mipId = req.query.mipId;
    }
    filter['computeFiles.InputColorDepthImage'] = { $exists: true };
    filter.tags = { $nin: ['junk', 'validationError'] };

    // Get total count
    const total = await collection.countDocuments(filter);

    // Get paginated results
    const neurons = await collection
      .find(filter)
      .skip(skip)
      .limit(limit)
      .sort({ publishedName: 1 })
      .toArray();

    res.json({
      data: serializeDocuments(neurons),
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    console.error('Error fetching neurons:', error);
    res.status(500).json({ error: 'Failed to fetch neurons' });
  }
});

/**
 * GET /api/neurons/:id
 * Get a specific neuron by ID
 */
router.get('/:id', async (req, res) => {
  try {
    const db = getDB();
    const collection = db.collection('neuronMetadata');

    console.debug(`Fetch neuron: ${req.params.id}`)
    const neuron = await collection.findOne({ _id: convertStringToId(req.params.id) });

    if (!neuron) {
      return res.status(404).json({ error: 'Neuron not found' });
    }

    res.json(serializeDocument(neuron));
  } catch (error) {
    console.error('Error fetching neuron:', error);
    res.status(500).json({ error: 'Failed to fetch neuron' });
  }
});

/**
 * GET /api/neurons/libraries
 * Get list of all libraries
 */
router.get('/meta/libraries', async (req, res) => {
  try {
    const db = getDB();
    const collection = db.collection('neuronMetadata');

    const filter = {};
    if (req.query.isEM === 'true') {
      filter.libraryName = { $regex: /^(flyem|flywire)/ };
    } else if (req.query.isEM === 'false') {
      filter.libraryName = { $not: /^(flyem|flywire)/ };
    }
    if (req.query.alignmentSpace) {
      filter.alignmentSpace = req.query.alignmentSpace;
    }
    const libraries = await collection.distinct('libraryName', filter);

    res.json(libraries.filter(lib => lib != null));
  } catch (error) {
    console.error('Error fetching libraries:', error);
    res.status(500).json({ error: 'Failed to fetch libraries' });
  }
});

/**
 * GET /api/neurons/alignment-spaces
 * Get list of all alignment spaces
 */
router.get('/meta/alignment-spaces', async (req, res) => {
  try {
    const db = getDB();
    const collection = db.collection('neuronMetadata');

    const spaces = await collection.distinct('alignmentSpace');

    res.json(spaces.filter(space => space != null));
  } catch (error) {
    console.error('Error fetching alignment spaces:', error);
    res.status(500).json({ error: 'Failed to fetch alignment spaces' });
  }
});

module.exports = router;
