const express = require('express');
const router = express.Router();
const path = require('path');
const fs = require('fs').promises;
const sharp = require('sharp');

const IMAGE_BASE_PATH = process.env.IMAGE_BASE_PATH || '';
const THUMBNAIL_WIDTH = parseInt(process.env.THUMBNAIL_WIDTH) || 200;
const THUMBNAIL_HEIGHT = parseInt(process.env.THUMBNAIL_HEIGHT) || 200;

/**
 * GET /api/images/thumbnail
 * Generate and serve thumbnail images for PNG and TIF files
 * Query params:
 *   - imagePath: path to the source image
 */
router.get('/thumbnail', async (req, res) => {
  try {

    if (!IMAGE_BASE_PATH) {
      return res.status(500).json({ error: 'IMAGE_BASE_PATH not configured' });
    }

    const imagePath = req.query.imagePath;

    if (!imagePath) {
      return res.status(400).json({ error: 'imagePath query parameter required' });
    }

    if (!imagePath.startsWith(IMAGE_BASE_PATH)) {
      return res.status(403).json({ error: `${imagePath} is not accessible` });
    }

    const ext = path.extname(imagePath).toLowerCase();

    // Check if file exists
    await fs.access(imagePath);

    // Generate thumbnail for PNG and TIF files
    if (ext === '.png' || ext === '.tif' || ext === '.tiff') {
      const thumbnail = await sharp(imagePath)
        .resize(THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, {
          fit: 'inside',
          withoutEnlargement: true
        })
        .png()
        .toBuffer();

      res.setHeader('Content-Type', 'image/png');
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.send(thumbnail);
    } else {
      // For other formats, serve the original file
      const contentType = ext === '.jpg' || ext === '.jpeg' ? 'image/jpeg' : 'image/png';
      res.setHeader('Content-Type', contentType);
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.sendFile(imagePath);
    }
  } catch (error) {
    console.error('Error serving thumbnail:', error);
    res.status(500).json({ error: 'Failed to serve thumbnail' });
  }
});

module.exports = router;
