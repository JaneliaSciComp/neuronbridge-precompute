const express = require('express');
const router = express.Router();
const { getDB } = require('../config/database');
const { convertStringToId, serializeDocuments } = require('../utils/serialization');

const DEFAULT_PAGE_SIZE = parseInt(process.env.DEFAULT_PAGE_SIZE) || 50;
const MAX_PAGE_SIZE = parseInt(process.env.MAX_PAGE_SIZE) || 500;

/**
 * GET /api/matches/em-cdms/:neuronId
 * Get color depth matches for an EM neuron
 * Query params:
 *   - page: page number (default: 0)
 *   - limit: items per page (default: 50)
 *   - minScore: minimum normalized score
 *   - targetLibraries: comma-separated list of target libraries (optional)
 */
router.get('/em-cdms/:neuronId', async (req, res) => {
  try {
    const db = getDB();
    const matchesCollection = db.collection('cdMatches');

    const page = parseInt(req.query.page) || 0;
    const limit = Math.min(parseInt(req.query.limit) || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
    const skip = page * limit;

    // Parse target libraries from query param
    const targetLibraries = req.query.targetLibraries
      ? req.query.targetLibraries.split(',').map(lib => lib.trim())
      : null;

    // Build pipeline
    const pipeline = [];
    const matchFilter = createMatchStage(req.query, {
        maskImageRefId: convertStringToId(req.params.neuronId),
    });
    pipeline.push(matchFilter);
    pipeline.push(...createLookupStage(targetLibraries, 'matchedImageRefId'));

    // Get total count
    const totalResult = await matchesCollection.aggregate([
      ...pipeline,
      { $count: 'count' },
    ]).toArray();
    const total = totalResult[0]?.count || 0;

    console.log(`Found ${total} matches using `, JSON.stringify(pipeline));

    pipeline.push({
      $sort: { normalizedScore: -1 },
    });

    pipeline.push({
      $skip: skip
    });

    pipeline.push({
      $limit: limit
    });

    // Get paginated matches
    console.debug('Get matches:', JSON.stringify(pipeline));
    const matches = await matchesCollection
      .aggregate(pipeline)
      .toArray();

    // Add negativeScore to matches with gradientAreaGap
    const matchesWithNegativeScore = addNegativeScore(matches);

    res.json({
      data: serializeDocuments(matchesWithNegativeScore),
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    console.error('Error fetching CDM matches:', error);
    res.status(500).json({ error: 'Failed to fetch matches' });
  }
});

/**
 * GET /api/matches/lm-cdms/:neuronId
 * Get color depth matches for an LM neuron
 * Query params:
 *   - page: page number (default: 0)
 *   - limit: items per page (default: 50)
 *   - minScore: minimum normalized score
 *   - targetLibraries: comma-separated list of target libraries (optional)
 */
router.get('/lm-cdms/:neuronId', async (req, res) => {
  try {
    const db = getDB();
    const matchesCollection = db.collection('cdMatches');

    const page = parseInt(req.query.page) || 0;
    const limit = Math.min(parseInt(req.query.limit) || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
    const skip = page * limit;

    // Parse target libraries from query param
    const targetLibraries = req.query.targetLibraries
      ? req.query.targetLibraries.split(',').map(lib => lib.trim())
      : null;

    // Build pipeline
    const pipeline = [];
    const matchFilter = createMatchStage(req.query, {
        matchedImageRefId: convertStringToId(req.params.neuronId),
    });

    pipeline.push(matchFilter);
    pipeline.push(...createLookupStage(targetLibraries, 'maskImageRefId'));

    // Get total count
    const totalResult = await matchesCollection.aggregate([
      ...pipeline,
      { $count: 'count' },
    ]).toArray();
    const total = totalResult[0]?.count || 0;

    console.log(`Found ${total} matches using `, JSON.stringify(pipeline));

    pipeline.push({
      $sort: { normalizedScore: -1 },
    });

    pipeline.push({
      $skip: skip
    });

    pipeline.push({
      $limit: limit
    });

    // Get paginated matches
    console.debug('Get matches:', JSON.stringify(pipeline));
    const matches = await matchesCollection
      .aggregate(pipeline)
      .toArray();

    // Add negativeScore to matches with gradientAreaGap
    const matchesWithNegativeScore = addNegativeScore(matches);

    res.json({
      data: serializeDocuments(matchesWithNegativeScore),
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    console.error('Error fetching CDM matches:', error);
    res.status(500).json({ error: 'Failed to fetch matches' });
  }
});

/**
 * GET /api/matches/em-cdms-stats/:neuronId
 * Get color depth match statistics for an EM neuron
 * Query params:
 *   - targetLibraries: comma-separated list of libraries (optional, returns all if not specified)
 *   - minScore: minimum normalized score
 */
router.get('/em-cdms-stats/:neuronId', async (req, res) => {
  try {
    const db = getDB();
    const matchesCollection = db.collection('cdMatches');

    // Parse target libraries from query param
    const targetLibraries = req.query.targetLibraries
      ? req.query.targetLibraries.split(',').map(lib => lib.trim())
      : null;

    // Build pipeline
    const pipeline = [];
    const matchFilter = createMatchStage({}/*no filtering by the scores*/, {
        maskImageRefId: convertStringToId(req.params.neuronId),
    });
    pipeline.push(matchFilter);

    // Lookup neuron metadata
    const lookupStage = targetLibraries
      ? {
          from: 'neuronMetadata',
          let: { imageId: '$matchedImageRefId' },
          pipeline: [
            {
              $match: {
                $expr: { $eq: [ '$_id', '$$imageId' ] },
                libraryName: { $in: targetLibraries },
              }
            }
          ],
          as: 'matchedImage'
        }
      : {
          from: 'neuronMetadata',
          localField: 'matchedImageRefId',
          foreignField: '_id',
          as: 'matchedImage'
        };

    pipeline.push({ $lookup: lookupStage });
    pipeline.push({
      $unwind: {
        path: '$matchedImage',
        preserveNullAndEmptyArrays: false,
      }
    });

    // Add group stage to compute max pixel score and max negative score per library
    pipeline.push({
      $group: {
        _id: '$matchedImage.libraryName',
        maxMatchingPixels: { $max: '$matchingPixels' },
        maxNegativeScore: {
          $max: {
            $cond: [
              {
                $and: [
                  { $ne: ['$gradientAreaGap', null] },
                  { $ne: ['$gradientAreaGap', -1] }
                ]
              },
              {
                $add: [
                  '$gradientAreaGap',
                  { $divide: [{ $ifNull: ['$highExpressionArea', 0] }, 3] }
                ]
              },
              null
            ]
          }
        }
      }
    });

    // Sort by library name
    pipeline.push({
      $sort: { _id: 1 }
    });

    // Execute aggregation
    const stats = await matchesCollection
      .aggregate(pipeline)
      .toArray();

    // Format results
    const results = stats.map(stat => ({
      targetLibrary: stat._id,
      maxMatchingPixels: stat.maxMatchingPixels || 0,
      maxNegativeScore: stat.maxNegativeScore || 0
    }));

    res.json({
      neuronId: req.params.neuronId,
      results
    });

  } catch (error) {
    console.error('Error fetching maximum values for neuron CDM scores:', error);
    res.status(500).json({ error: 'Failed to fetch max neuron scores' });
  }
});

const createMatchStage = (query, neuronReferenceFilter) => {
  const filter = {
    ...neuronReferenceFilter,
  };
  if (query.minScore) {
    filter.normalizedScore = { $gte: parseFloat(query.minScore) };
  }
  if (query.minMatchingPixels) {
    filter.matchingPixels = { $gte: parseInt(query.minMatchingPixels) }
  }
  if (query.withGradScore) {
    filter.gradientAreaGap = { $exists: true };
  }
  console.debug('Matches filter:', filter);
  return { 
    $match: filter
  };
};

const createLookupStage = (targetLibraries, referenceField) => {
  const lookup = targetLibraries && targetLibraries.length > 0
    ? {
        from: 'neuronMetadata',
        let: { imageId: '$' + referenceField },
        pipeline: [
          {
            $match: {
              $expr: { $eq: [ '$_id', '$$imageId' ] },
              libraryName: { $in: targetLibraries },
            }
          }
        ],
        as: 'matchedImage'
      }
    : {
        from: 'neuronMetadata',
        localField: referenceField,
        foreignField: '_id',
        as: 'matchedImage'
      }
  return [
    { $lookup: lookup },
    {
      $unwind: {
          path: '$matchedImage',
          preserveNullAndEmptyArrays: false,
      }
    },
  ]
};

const addNegativeScore = (matches) => {
  return matches.map(match => {
    // Add negativeScore if gradientAreaGap exists
    if (match.gradientAreaGap !== null && match.gradientAreaGap !== undefined && match.gradientAreaGap !== -1) {
      const highExpressionArea = match.highExpressionArea || 0;
      match.negativeScore = match.gradientAreaGap + (highExpressionArea / 3);
    }
    return match;
  });
}

module.exports = router;
