const express = require('express');
const router = express.Router();
const { getDB } = require('../config/database');

const DEFAULT_PAGE_SIZE = parseInt(process.env.DEFAULT_PAGE_SIZE) || 50;
const MAX_PAGE_SIZE = parseInt(process.env.MAX_PAGE_SIZE) || 500;

/**
 * POST /api/search
 * Search neurons with complex criteria
 * Body:
 *   - publishedName: string or regex
 *   - libraryName: string or array
 *   - alignmentSpace: string
 *   - neuronTerms: array of terms
 *   - mipId: string
 *   - page: number
 *   - limit: number
 */
router.post('/', async (req, res) => {
  try {
    const db = getDB();
    const collection = db.collection('neuronMetadata');

    const page = parseInt(req.body.page) || 0;
    const limit = Math.min(parseInt(req.body.limit) || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
    const skip = page * limit;

    // Build search filter
    const filter = {};

    if (req.body.publishedName) {
      filter.publishedName = { $regex: req.body.publishedName, $options: 'i' };
    }

    if (req.body.libraryName) {
      if (Array.isArray(req.body.libraryName)) {
        filter.libraryName = { $in: req.body.libraryName };
      } else {
        filter.libraryName = req.body.libraryName;
      }
    }

    if (req.body.alignmentSpace) {
      filter.alignmentSpace = req.body.alignmentSpace;
    }

    if (req.body.neuronTerms && Array.isArray(req.body.neuronTerms)) {
      filter.neuronTerms = { $in: req.body.neuronTerms };
    }

    if (req.body.mipId) {
      filter.mipId = req.body.mipId;
    }

    if (req.body.sourceRefId) {
      filter.sourceRefId = req.body.sourceRefId;
    }

    // Get total count
    const total = await collection.countDocuments(filter);

    // Get results
    const neurons = await collection
      .find(filter)
      .skip(skip)
      .limit(limit)
      .sort({ publishedName: 1 })
      .toArray();

    res.json({
      data: neurons,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      },
      filter: filter
    });
  } catch (error) {
    console.error('Error searching neurons:', error);
    res.status(500).json({ error: 'Failed to search neurons' });
  }
});

/**
 * GET /api/search/autocomplete
 * Autocomplete for neuron names
 * Query params:
 *   - q: search query
 *   - field: field to search (publishedName, mipId, sourceRefId)
 *   - limit: max results (default: 10)
 */
router.get('/autocomplete', async (req, res) => {
  try {
    const db = getDB();
    const collection = db.collection('neuronMetadata');

    const query = req.query.q || '';
    const field = req.query.field || 'publishedName';
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);

    if (!query) {
      return res.json([]);
    }

    const filter = {};
    filter[field] = { $regex: `^${query}`, $options: 'i' };

    const results = await collection
      .find(filter, { projection: { [field]: 1, _id: 1 } })
      .limit(limit)
      .toArray();

    const suggestions = results.map(r => ({
      id: r._id,
      value: r[field]
    }));

    res.json(suggestions);
  } catch (error) {
    console.error('Error in autocomplete:', error);
    res.status(500).json({ error: 'Failed to get suggestions' });
  }
});

module.exports = router;
