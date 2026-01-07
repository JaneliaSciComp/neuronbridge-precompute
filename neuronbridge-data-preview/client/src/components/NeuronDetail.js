import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { isEM } from '../utils/neuronUtils'

function NeuronDetail() {
  const { id } = useParams();
  const [neuron, setNeuron] = useState(null);
  const [cdMatches, setCDMatches] = useState([]);
  const [loading, setLoading] = useState(true);
  const [matchesLoading, setMatchesLoading] = useState(false);
  const [error, setError] = useState(null);
  const [libraries, setLibraries] = useState([]);
  const [page, setPage] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [filters, setFilters] = useState({
    minNormalizedScore: '',
    minMatchingPixels: '',
    hasGradientAreaGap: false,
    targetMipId: '',
    targetLibrary: ''
  });

  useEffect(() => {
    // Reset filters and state when navigating to a new neuron
    setFilters({
      targetMipId: '',
      targetLibrary: ''
    });
    setCDMatches([]);
    setPage(0);
    setNeuron(null);
    fetchNeuron();
    // Scroll to top when navigating to new neuron
    window.scrollTo(0, 0);
  }, [id]);

  useEffect(() => {
    if (neuron) {
      fetchTargetLibraries(!isEM(neuron));
      fetchCDMatches(isEM(neuron));
    }
  }, [neuron, page]);

  const fetchNeuron = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`/api/neurons/${id}`);
      if (!response.ok) throw new Error('Neuron not found');

      const data = await response.json();
      setNeuron(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchTargetLibraries = async (isEM) => {
    try {
      const endpoint = isEM
        ? '/api/neurons/meta/libraries?isEM=true'
        : '/api/neurons/meta/libraries?isEM=false';
      const response = await fetch(endpoint);
      const data = await response.json();
      setLibraries(data);
    } catch (err) {
      console.error('Failed to fetch libraries:', err);
    }
  };

  const fetchCDMatches = async (isEM) => {
    setMatchesLoading(true);

    try {
      const endpoint = isEM
        ? `/api/cdmatches/em-cdms/${id}`
        : `/api/cdmatches/lm-cdms/${id}`;

      // Build query params starting with page and limit
      const params = new URLSearchParams({
        page: page,
        limit: '50'
      });

      // Add optional filters
      if (filters.minNormalizedScore && filters.minNormalizedScore !== '') {
        params.append('minScore', parseFloat(filters.minNormalizedScore));
      }
      if (filters.minMatchingPixels && filters.minMatchingPixels !== '') {
        params.append('minMatchingPixels', parseInt(filters.minMatchingPixels));
      }
      if (filters.hasGradientAreaGap) {
        params.append('withGradScore', 'true');
      }
      if (filters.targetMipId && filters.targetMipId !== '') {
        params.append('targetMipId', filters.targetMipId);
      }
      if (filters.targetLibrary && filters.targetLibrary !== '') {
        params.append('targetLibrary', filters.targetLibrary);
      }

      const response = await fetch(`${endpoint}?${params.toString()}`);
      if (!response.ok) throw new Error('Failed to fetch matches');

      const data = await response.json();

      setCDMatches(data.data);
      setTotalPages(data.pagination.totalPages);
    } catch (err) {
      console.error('Failed to fetch matches:', err);
      setCDMatches([]);
    } finally {
      setMatchesLoading(false);
    }
  };

  const getImageUrl = (neuron, fileType = 'SourceColorDepthImage') => {
    const image = neuron?.computeFiles?.[fileType] ||
                  neuron?.computeFiles?.InputColorDepthImage;

    if (image) {
      return `/api/images/thumbnail?imagePath=${image}`;
    }
    return null;
  };

  if (loading) {
    return <div className="loading">Loading neuron details...</div>;
  }

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  if (!neuron) {
    return <div className="error">Neuron not found</div>;
  }

  return (
    <div className="detail-container">
      <div className="detail-header">
        <Link to="/">← Back to List</Link>
        <h1>{neuron.publishedName}</h1>
      </div>

      <div style={{ display: 'flex', gap: '30px', marginBottom: '30px', alignItems: 'flex-start' }}>
        {getImageUrl(neuron, 'InputColorDepthImage') && (
          <img
            src={getImageUrl(neuron, 'InputColorDepthImage')}
            alt={neuron.publishedName}
            className="detail-image"
            style={{ margin: 0, flex: '0 0 auto' }}
          />
        )}

        <div style={{
          flex: '1',
          padding: '20px',
          backgroundColor: '#f9f9f9',
          borderRadius: '4px',
          display: 'flex',
          flexDirection: 'column',
          gap: '10px'
        }}>
          {isEM(neuron) ? (
            <div style={{ fontSize: '14px', color: '#333' }}>
              <span style={{ fontWeight: 'bold', color: '#666' }}>Body ID:</span> {neuron.publishedName || 'N/A'}
            </div>
          ) : (
            <>
              <div style={{ fontSize: '14px', color: '#333' }}>
                <span style={{ fontWeight: 'bold', color: '#666' }}>Slide Code:</span> {neuron.slideCode || 'N/A'}
              </div>
              <div style={{ fontSize: '14px', color: '#333' }}>
                <span style={{ fontWeight: 'bold', color: '#666' }}>Channel:</span> {neuron.channel || 'N/A'}
              </div>
              <div style={{ fontSize: '14px', color: '#333' }}>
                <span style={{ fontWeight: 'bold', color: '#666' }}>Objective:</span> {neuron.objective || 'N/A'}
              </div>
              {neuron.datasetLabels && neuron.datasetLabels.length > 0 && (
                <div style={{ fontSize: '14px', color: '#333' }}>
                  <span style={{ fontWeight: 'bold', color: '#666' }}>Dataset:</span> {neuron.datasetLabels.join(', ')}
                </div>
              )}
            </>
          )}

          <div style={{ fontSize: '14px', color: '#333' }}>
            <span style={{ fontWeight: 'bold', color: '#666' }}>MIP ID:</span> {neuron.mipId || 'N/A'}
          </div>

          <div style={{ fontSize: '14px', color: '#333' }}>
            <span style={{ fontWeight: 'bold', color: '#666' }}>Library:</span> {neuron.libraryName || 'N/A'}
          </div>

          <div style={{ fontSize: '14px', color: '#333' }}>
            <span style={{ fontWeight: 'bold', color: '#666' }}>Alignment Space:</span> {neuron.alignmentSpace || 'N/A'}
          </div>

          <div style={{ fontSize: '14px', color: '#333' }}>
            <span style={{ fontWeight: 'bold', color: '#666' }}>Source Ref ID:</span> {neuron.sourceRefId || 'N/A'}
          </div>
        </div>
      </div>

      {neuron.neuronTerms && neuron.neuronTerms.length > 0 && (
        <div className="metadata-item" style={{ marginBottom: '20px' }}>
          <div className="metadata-label">Neuron Terms</div>
          <div className="metadata-value">{neuron.neuronTerms.join(', ')}</div>
        </div>
      )}

      <div className="section">
        <h2>Matches</h2>

        <div style={{
          background: '#f8f9fa',
          border: '1px solid #dee2e6',
          borderRadius: '8px',
          padding: '20px',
          marginBottom: '24px'
        }}>
          <div style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '16px',
            alignItems: 'flex-end'
          }}>
            <div style={{ flex: '1 1 200px', minWidth: '200px' }}>
              <label style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: '500',
                color: '#495057'
              }}>
                Min Normalized Score
              </label>
              <input
                type="number"
                step="0.01"
                placeholder="e.g., 0.5"
                value={filters.minNormalizedScore}
                onChange={(e) => setFilters({ ...filters, minNormalizedScore: e.target.value })}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px',
                  fontSize: '14px'
                }}
              />
            </div>

            <div style={{ flex: '1 1 200px', minWidth: '200px' }}>
              <label style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: '500',
                color: '#495057'
              }}>
                Min Matching Pixels
              </label>
              <input
                type="number"
                placeholder="e.g., 1000"
                value={filters.minMatchingPixels}
                onChange={(e) => setFilters({ ...filters, minMatchingPixels: e.target.value })}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px',
                  fontSize: '14px'
                }}
              />
            </div>

            <div style={{ flex: '1 1 200px', minWidth: '200px' }}>
              <label style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: '500',
                color: '#495057'
              }}>
                Target MIP ID
              </label>
              <input
                type="text"
                placeholder="e.g., 123456"
                value={filters.targetMipId}
                onChange={(e) => setFilters({ ...filters, targetMipId: e.target.value })}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px',
                  fontSize: '14px'
                }}
              />
            </div>

            <div style={{ flex: '1 1 200px', minWidth: '200px' }}>
              <label style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: '500',
                color: '#495057'
              }}>
                Target Library
              </label>
              <select
                value={filters.targetLibrary}
                onChange={(e) => setFilters({ ...filters, targetLibrary: e.target.value })}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px',
                  fontSize: '14px',
                  backgroundColor: '#fff',
                  cursor: 'pointer'
                }}
              >
                <option value="">All Libraries</option>
                {libraries.map(lib => (
                  <option key={lib} value={lib}>{lib}</option>
                ))}
              </select>
            </div>

            <div style={{
              flex: '0 1 auto',
              display: 'flex',
              alignItems: 'center',
              paddingBottom: '8px'
            }}>
              <label style={{
                display: 'flex',
                alignItems: 'center',
                cursor: 'pointer',
                fontSize: '14px',
                fontWeight: '500',
                color: '#495057'
              }}>
                <input
                  type="checkbox"
                  checked={filters.hasGradientAreaGap}
                  onChange={(e) => setFilters({ ...filters, hasGradientAreaGap: e.target.checked })}
                  style={{
                    marginRight: '8px',
                    width: '16px',
                    height: '16px',
                    cursor: 'pointer'
                  }}
                />
                Has Gradient Score
              </label>
            </div>

            <div style={{
              flex: '0 1 auto',
              display: 'flex',
              gap: '10px',
              marginLeft: 'auto'
            }}>
              <button
                onClick={() => {
                  if (page === 0) {
                    fetchCDMatches(isEM(neuron));
                  } else {
                    setPage(0);
                  }
                }}
                style={{
                  backgroundColor: '#1976d2',
                  color: '#fff',
                  border: 'none',
                  padding: '10px 20px',
                  borderRadius: '4px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'background-color 0.2s'
                }}
                onMouseOver={(e) => e.target.style.backgroundColor = '#1565c0'}
                onMouseOut={(e) => e.target.style.backgroundColor = '#1976d2'}
              >
                Get Matches
              </button>
              <button
                onClick={() => {
                  setFilters({ minNormalizedScore: '', minMatchingPixels: '', hasGradientAreaGap: false, targetMipId: '', targetLibrary: '' });
                  setPage(0);
                }}
                style={{
                  backgroundColor: '#fff',
                  color: '#6c757d',
                  border: '1px solid #6c757d',
                  padding: '10px 20px',
                  borderRadius: '4px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'all 0.2s'
                }}
                onMouseOver={(e) => {
                  e.target.style.backgroundColor = '#6c757d';
                  e.target.style.color = '#fff';
                }}
                onMouseOut={(e) => {
                  e.target.style.backgroundColor = '#fff';
                  e.target.style.color = '#6c757d';
                }}
              >
                Clear Filters
              </button>
            </div>
          </div>
        </div>

        {matchesLoading ? (
          <div className="loading">Loading matches...</div>
        ) : cdMatches.length > 0 ? (
          <>
            <div className="match-grid">
              {cdMatches.map((match, idx) => {
                const matchedNeuron = match.matchedImage;
                return (
                  <div key={idx} className="match-card">
                    {getImageUrl(matchedNeuron) && (
                      <img
                        src={getImageUrl(matchedNeuron)}
                        alt={matchedNeuron?.publishedName}
                      />
                    )}
                    {match.normalizedScore && (
                      <div className="match-score">
                        Score: {match.normalizedScore.toFixed(2)}
                      </div>
                    )}
                    <div><strong>{matchedNeuron?.publishedName}</strong></div>
                    <div style={{ fontSize: '12px', color: '#999' }}>
                      {matchedNeuron?.libraryName}
                    </div>
                    {isEM(matchedNeuron)
                      ? (
                          <div style={{ fontSize: '12px', color: '#999' }}>
                            BodyID: {matchedNeuron?.publishedName}
                          </div>
                      )
                      : (
                          <div style={{ fontSize: '12px', color: '#999' }}>
                            SlideCode: {matchedNeuron?.slideCode}
                          </div>
                      )
                    }
                    {match.matchingPixels && (
                      <div style={{ fontSize: '11px', color: '#999' }}>
                        Pixels: {match.matchingPixels.toLocaleString()}
                      </div>
                    )}
                    {match.gradientAreaGap !== null && match.gradientAreaGap !== undefined && match.gradientAreaGap !== -1 && (
                      <div>
                        <div style={{ fontSize: '11px', color: '#999' }}>
                          Negative Score: {match.negativeScore.toFixed(2)}
                        </div>
                        <div style={{ fontSize: '11px', color: '#999', paddingLeft: '12px' }}>
                          Gradient Gap: {match.gradientAreaGap.toFixed(2)}
                        </div>
                        <div style={{ fontSize: '11px', color: '#999', paddingLeft: '12px' }}>
                          High Expression: {match.highExpressionArea.toFixed(2)}
                        </div>
                      </div>
                    )}
                    {matchedNeuron?._id && (
                      <Link to={`/neuron/${matchedNeuron._id}`} style={{ fontSize: '12px' }}>
                        View Details →
                      </Link>
                    )}
                  </div>
                );
              })}
            </div>

            <div className="pagination">
              <button
                onClick={() => setPage(Math.max(0, page - 1))}
                disabled={page === 0}
              >
                Previous
              </button>
              <span>Page {page + 1} of {totalPages}</span>
              <button
                onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                disabled={page >= totalPages - 1}
              >
                Next
              </button>
            </div>
          </>
        ) : (
          <p>No color depth matches found.</p>
        )}
      </div>
    </div>
  );
}

export default NeuronDetail;
