/**
 * Utility functions for serializing MongoDB data for JSON responses
 * or preparing it for a query
 */

const { Long } = require('bson');


const convertStringToId = (sid) => Long.fromString(sid);

/**
 * Convert MongoDB Long _id to string
 * MongoDB Long is represented as { high: number, low: number }
 */
const convertIdToString = (id) => {
  if (!id) return id;
  if (typeof id === 'string') return id;

  // MongoDB Long type with high/low fields
  if (typeof id === 'object' && 'high' in id && 'low' in id) {
    // For 64-bit: (high * 2^32) + low
    const bigIntValue = (BigInt(id.high) << 32n) | BigInt(id.low >>> 0);
    return bigIntValue.toString();
  }

  // MongoDB ObjectId or other types with toString
  if (typeof id === 'object' && typeof id.toString === 'function') {
    return id.toString();
  }

  return String(id);
};

const ID_NAMES = [
  '_id',
  'maskImageRefId',
  'matchedImageRefId',
  'sessionRefId',
];

/**
 * Recursively serialize all _id fields in an object
 * Useful for nested objects with _id fields
 */
const deepSerialize = (obj) => {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map(deepSerialize);
  }

  const serialized = {};
  for (const [key, value] of Object.entries(obj)) {
    if (ID_NAMES.includes(key)) {
      serialized[key] = convertIdToString(value);
    } else if (typeof value === 'object' && value !== null) {
      serialized[key] = deepSerialize(value);
    } else {
      serialized[key] = value;
    }
  }

  return serialized;
};

/**
 * Serialize a single document by converting _id to string
 */
const serializeDocument = deepSerialize

/**
 * Serialize an array of documents by converting all _id fields to strings
 */
const serializeDocuments = (docs) => {
  if (!Array.isArray(docs)) return docs;

  return docs.map(serializeDocument);
};

module.exports = {
  convertStringToId,
  convertIdToString,
  serializeDocument,
  serializeDocuments,
};
