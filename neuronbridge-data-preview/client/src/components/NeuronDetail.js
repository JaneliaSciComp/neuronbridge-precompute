import { useState, useEffect, useRef } from 'react';
import { useParams, Link, useLocation } from 'react-router-dom';
import { isEM } from '../utils/neuronUtils';
import { CoordsProvider } from '../contexts/MouseCoordsContext';
import MousePosition from './MousePosition';

function NeuronDetail() {
  const { id } = useParams();
  const location = useLocation();
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
    targetLibraries: []
  });
  const [maxStats, setMaxStats] = useState([]);
  const [modalMatchIndex, setModalMatchIndex] = useState(null);
  const [imageDimensions, setImageDimensions] = useState({ current: null, matched: null });
  const [libraryDropdownOpen, setLibraryDropdownOpen] = useState(false);
  const currentImgRef = useRef(null);
  const matchedImgRef = useRef(null);
  const libraryDropdownRef = useRef(null);

  // Derive modalMatch from index
  const modalMatch = modalMatchIndex !== null ? cdMatches[modalMatchIndex] : null;

  useEffect(() => {
    // Reset filters and state when navigating to a new neuron
    setFilters({
      targetMipId: '',
      targetLibraries: []
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
      fetchTargetLibraries();
      fetchCDMatches(isEM(neuron));
    }
  }, [neuron, page]);

  useEffect(() => {
    if (neuron && isEM(neuron)) {
      fetchMaxStats();
    }
  }, [neuron, filters.targetLibraries]);

  useEffect(() => {
    // Reset image dimensions when modal is closed
    if (modalMatchIndex === null) {
      setImageDimensions({ current: null, matched: null });
    }
  }, [modalMatchIndex]);

  // Keyboard navigation for modal
  useEffect(() => {
    if (modalMatchIndex === null) return;

    function handleKeyDown({ key }) {
      if (/^Arrow(Left|Up)$/.test(key)) {
        setModalMatchIndex(prev => (prev > 0 ? prev - 1 : prev));
      }
      if (/^Arrow(Right|Down)$/.test(key)) {
        setModalMatchIndex(prev => (prev < cdMatches.length - 1 ? prev + 1 : prev));
      }
      if (key === 'Escape') {
        setModalMatchIndex(null);
      }
    }

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [modalMatchIndex, cdMatches.length]);

  // Close library dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (libraryDropdownRef.current && !libraryDropdownRef.current.contains(event.target)) {
        setLibraryDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const toggleLibrary = (lib) => {
    setFilters(prev => ({
      ...prev,
      targetLibraries: prev.targetLibraries.includes(lib)
        ? prev.targetLibraries.filter(l => l !== lib)
        : [...prev.targetLibraries, lib]
    }));
  };

  const removeLibrary = (lib) => {
    setFilters(prev => ({
      ...prev,
      targetLibraries: prev.targetLibraries.filter(l => l !== lib)
    }));
  };

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

  const fetchTargetLibraries = async () => {
    try {
      const endpoint = '/api/neurons/meta/libraries';
      const response = await fetch(endpoint);
      const data = await response.json();
      setLibraries(data);
    } catch (err) {
      console.error('Failed to fetch libraries:', err);
    }
  };

  const fetchMaxStats = async () => {
    try {
      const endpoint = `/api/cdmatches/em-cdms-stats/${id}`;
      const params = new URLSearchParams();

      if (filters.targetLibraries && filters.targetLibraries.length > 0) {
        params.append('targetLibraries', filters.targetLibraries.join(','));
      }

      const url = params.toString() ? `${endpoint}?${params.toString()}` : endpoint;
      const response = await fetch(url);

      if (!response.ok) throw new Error('Failed to fetch max stats');

      const data = await response.json();
      setMaxStats(data.results || []);
    } catch (err) {
      console.error('Failed to fetch max stats:', err);
      setMaxStats([]);
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
      if (filters.targetLibraries && filters.targetLibraries.length > 0) {
        params.append('targetLibraries', filters.targetLibraries.join(','));
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

  const getImageUrl = (neuron, fileType = 'SourceColorDepthImage', size = null) => {
    const image = neuron?.computeFiles?.[fileType] ||
                  neuron?.computeFiles?.InputColorDepthImage;

    if (image) {
      // Use custom size if provided, otherwise use larger default for detail page (600x600)
      const width = size || 600;
      const height = size || 600;
      return `/api/images/thumbnail?imagePath=${image}&width=${width}&height=${height}`;
    }
    return null;
  };

  const formatLibraryName = (libraryName) => {
    if (!libraryName) return '';
    // Remove prefix before first underscore
    let formatted = libraryName.substring(libraryName.indexOf('_') + 1);
    // Remove '_published' from the name
    formatted = formatted.replace('_published', '');
    return formatted;
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

  // Get the search params from the location state (passed from NeuronList)
  const backToListUrl = location.state?.fromSearch ? `/${location.state.fromSearch}` : '/';

  return (
    <div className="detail-container">
      <div className="detail-header">
        <Link to={backToListUrl}>← Back to List</Link>
        <h1>{neuron.publishedName}</h1>
      </div>

      <div style={{ display: 'flex', gap: '30px', marginBottom: '30px', alignItems: 'flex-start' }}>
        {getImageUrl(neuron, 'InputColorDepthImage') && (
          <img
            src={getImageUrl(neuron, 'InputColorDepthImage')}
            alt={neuron.publishedName}
            className="detail-image"
            style={{
              margin: 0,
              flex: '0 0 auto',
              maxWidth: neuron.alignmentSpace === 'JRC2018_VNC_Unisex_40x_DS' ? '300px' : '600px'
            }}
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

      {isEM(neuron) && maxStats.length > 0 && (
        <div style={{
          background: '#f0f7ff',
          border: '1px solid #b3d9ff',
          borderRadius: '8px',
          padding: '20px',
          marginBottom: '20px'
        }}>
          <h3 style={{ marginTop: '0', marginBottom: '15px', fontSize: '16px', color: '#333' }}>
            Match Statistics
          </h3>
          <div style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '12px'
          }}>
            {maxStats.map((stat, idx) => (
              <div key={idx} style={{
                flex: '1 1 200px',
                minWidth: '200px',
                padding: '12px',
                backgroundColor: '#fff',
                borderRadius: '6px',
                border: '1px solid #b3d9ff'
              }}>
                <div style={{ fontSize: '14px', fontWeight: '600', color: '#1976d2', marginBottom: '8px' }}>
                  {formatLibraryName(stat.targetLibrary)}
                </div>
                <div style={{ fontSize: '13px', color: '#333', marginBottom: '4px' }}>
                  <span style={{ fontWeight: '500' }}>Max Pixels:</span> {stat.maxMatchingPixels.toLocaleString()}
                </div>
                {stat.maxNegativeScore > 0 && (
                  <div style={{ fontSize: '13px', color: '#333' }}>
                    <span style={{ fontWeight: '500' }}>Max Negative Score:</span> {stat.maxNegativeScore.toFixed(2)}
                  </div>
                )}
              </div>
            ))}
          </div>
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

            <div style={{ flex: '1 1 250px', minWidth: '250px', position: 'relative' }} ref={libraryDropdownRef}>
              <label style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: '500',
                color: '#495057'
              }}>
                Target Libraries
              </label>
              <div
                onClick={() => setLibraryDropdownOpen(!libraryDropdownOpen)}
                style={{
                  width: '100%',
                  minHeight: '38px',
                  padding: '4px 30px 4px 8px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px',
                  fontSize: '14px',
                  backgroundColor: '#fff',
                  cursor: 'pointer',
                  display: 'flex',
                  flexWrap: 'wrap',
                  gap: '4px',
                  alignItems: 'center',
                  position: 'relative'
                }}
              >
                {filters.targetLibraries.length === 0 ? (
                  <span style={{ color: '#6c757d' }}>All libraries</span>
                ) : (
                  filters.targetLibraries.map(lib => (
                    <span
                      key={lib}
                      style={{
                        backgroundColor: '#e9ecef',
                        padding: '2px 6px',
                        borderRadius: '3px',
                        fontSize: '12px',
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: '4px',
                        maxWidth: '150px'
                      }}
                    >
                      <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {lib}
                      </span>
                      <span
                        onClick={(e) => {
                          e.stopPropagation();
                          removeLibrary(lib);
                        }}
                        style={{
                          cursor: 'pointer',
                          fontWeight: 'bold',
                          color: '#6c757d',
                          lineHeight: 1
                        }}
                      >
                        ×
                      </span>
                    </span>
                  ))
                )}
                <span style={{
                  position: 'absolute',
                  right: '10px',
                  top: '50%',
                  transform: 'translateY(-50%)',
                  color: '#6c757d',
                  fontSize: '10px'
                }}>
                  ▼
                </span>
              </div>
              {libraryDropdownOpen && (
                <div style={{
                  position: 'absolute',
                  top: '100%',
                  left: 0,
                  right: 0,
                  backgroundColor: '#fff',
                  border: '1px solid #ced4da',
                  borderRadius: '4px',
                  marginTop: '2px',
                  maxHeight: '200px',
                  overflowY: 'auto',
                  zIndex: 1000,
                  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
                }}>
                  {libraries.map(lib => (
                    <div
                      key={lib}
                      onClick={() => toggleLibrary(lib)}
                      style={{
                        padding: '8px 12px',
                        cursor: 'pointer',
                        backgroundColor: filters.targetLibraries.includes(lib) ? '#e3f2fd' : 'transparent',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px'
                      }}
                      onMouseOver={(e) => {
                        if (!filters.targetLibraries.includes(lib)) {
                          e.currentTarget.style.backgroundColor = '#f8f9fa';
                        }
                      }}
                      onMouseOut={(e) => {
                        e.currentTarget.style.backgroundColor = filters.targetLibraries.includes(lib) ? '#e3f2fd' : 'transparent';
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={filters.targetLibraries.includes(lib)}
                        onChange={() => {}}
                        style={{ margin: 0, cursor: 'pointer' }}
                      />
                      <span style={{ fontSize: '13px' }}>{lib}</span>
                    </div>
                  ))}
                </div>
              )}
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
                  setFilters({ minNormalizedScore: '', minMatchingPixels: '', hasGradientAreaGap: false, targetMipId: '', targetLibraries: [] });
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
                    {getImageUrl(matchedNeuron, 'SourceColorDepthImage', 200) && (
                      <img
                        src={getImageUrl(matchedNeuron, 'SourceColorDepthImage', 200)}
                        alt={matchedNeuron?.publishedName}
                      />
                    )}
                    {match.normalizedScore && (
                      <div className="match-score">
                        Score: {match.normalizedScore.toFixed(2)}
                      </div>
                    )}
                    <div>
                      <strong>
                        {matchedNeuron?._id ? (
                          <Link
                            to={`/neuron/${matchedNeuron._id}`}
                            state={{ fromSearch: location.state?.fromSearch }}
                            style={{
                              color: '#1976d2',
                              textDecoration: 'none'
                            }}
                          >
                            {matchedNeuron?.publishedName}
                          </Link>
                        ) : (
                          matchedNeuron?.publishedName
                        )}
                      </strong>
                    </div>
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
                    <a
                      href="#"
                      onClick={(e) => {
                        e.preventDefault();
                        setModalMatchIndex(idx);
                      }}
                      style={{
                        fontSize: '12px',
                        color: '#1976d2',
                        textDecoration: 'none',
                        cursor: 'pointer',
                        marginTop: '8px',
                        display: 'inline-block'
                      }}
                      onMouseOver={(e) => e.target.style.textDecoration = 'underline'}
                      onMouseOut={(e) => e.target.style.textDecoration = 'none'}
                    >
                      View Details →
                    </a>
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

      {modalMatch && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000,
            padding: '20px'
          }}
          onClick={() => setModalMatchIndex(null)}
        >
          <div
            style={{
              backgroundColor: '#fff',
              borderRadius: '8px',
              padding: '30px',
              maxWidth: '1400px',
              maxHeight: '90vh',
              overflow: 'auto',
              position: 'relative'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <button
              onClick={() => setModalMatchIndex(null)}
              style={{
                position: 'absolute',
                top: '10px',
                right: '10px',
                background: '#fff',
                border: '1px solid #ccc',
                borderRadius: '4px',
                padding: '8px 12px',
                cursor: 'pointer',
                fontSize: '16px',
                fontWeight: 'bold',
                color: '#666'
              }}
              onMouseOver={(e) => {
                e.target.style.backgroundColor = '#f0f0f0';
              }}
              onMouseOut={(e) => {
                e.target.style.backgroundColor = '#fff';
              }}
            >
              ✕
            </button>

            <h2 style={{ marginTop: 0, marginBottom: '10px', textAlign: 'center' }}>
              Compare Neurons
            </h2>
            <div style={{ textAlign: 'center', marginBottom: '20px', fontSize: '14px', color: '#666' }}>
              Match {modalMatchIndex + 1} of {cdMatches.length}
            </div>

            <CoordsProvider>
              <div style={{ display: 'flex', gap: '30px', alignItems: 'flex-start' }}>
                <div style={{ flex: 1, textAlign: 'center' }}>
                  <h3 style={{ marginTop: 0, marginBottom: '15px', color: '#666' }}>
                    Current Neuron
                  </h3>
                  <div style={{ marginBottom: '10px', fontSize: '14px', fontWeight: 'bold' }}>
                    {neuron.publishedName}
                  </div>
                  <div style={{ marginBottom: '15px', fontSize: '12px', color: '#999' }}>
                    {neuron.libraryName}
                  </div>
                  {getImageUrl(neuron, 'InputColorDepthImage') && (
                    <div className="image-with-crosshair">
                      <img
                        ref={currentImgRef}
                        src={getImageUrl(neuron, 'InputColorDepthImage')}
                        alt={neuron.publishedName}
                        style={{
                          maxWidth: '600px',
                          width: '100%',
                          height: 'auto',
                          border: '1px solid #ddd',
                          borderRadius: '4px'
                        }}
                        onLoad={(e) => {
                          setImageDimensions(prev => ({
                            ...prev,
                            current: {
                              width: e.target.naturalWidth,
                              height: e.target.naturalHeight
                            }
                          }));
                        }}
                      />
                      {imageDimensions.current && (
                        <MousePosition
                          width={imageDimensions.current.width}
                          height={imageDimensions.current.height}
                        />
                      )}
                    </div>
                  )}
                </div>

                <div style={{ flex: 1, textAlign: 'center' }}>
                  <h3 style={{ marginTop: 0, marginBottom: '15px', color: '#666' }}>
                    Matched Neuron
                  </h3>
                  <div style={{ marginBottom: '10px', fontSize: '14px', fontWeight: 'bold' }}>
                    {modalMatch.matchedImage?.publishedName}
                  </div>
                  <div style={{ marginBottom: '15px', fontSize: '12px', color: '#999' }}>
                    {modalMatch.matchedImage?.libraryName}
                  </div>
                  {getImageUrl(modalMatch.matchedImage, 'InputColorDepthImage') && (
                    <div className="image-with-crosshair">
                      <img
                        ref={matchedImgRef}
                        src={getImageUrl(modalMatch.matchedImage, 'InputColorDepthImage')}
                        alt={modalMatch.matchedImage?.publishedName}
                        style={{
                          maxWidth: '600px',
                          width: '100%',
                          height: 'auto',
                          border: '1px solid #ddd',
                          borderRadius: '4px'
                        }}
                        onLoad={(e) => {
                          setImageDimensions(prev => ({
                            ...prev,
                            matched: {
                              width: e.target.naturalWidth,
                              height: e.target.naturalHeight
                            }
                          }));
                        }}
                      />
                      {imageDimensions.matched && (
                        <MousePosition
                          width={imageDimensions.matched.width}
                          height={imageDimensions.matched.height}
                        />
                      )}
                    </div>
                  )}
                  {modalMatch.normalizedScore && (
                    <div style={{ marginTop: '15px', fontSize: '14px', color: '#333' }}>
                      <strong>Match Score:</strong> {modalMatch.normalizedScore.toFixed(2)}
                    </div>
                  )}
                  {modalMatch.matchingPixels && (
                    <div style={{ fontSize: '13px', color: '#666' }}>
                      <strong>Matching Pixels:</strong> {modalMatch.matchingPixels.toLocaleString()}
                    </div>
                  )}
                  {modalMatch.gradientAreaGap !== null && modalMatch.gradientAreaGap !== undefined && modalMatch.gradientAreaGap !== -1 && (
                    <div style={{ marginTop: '10px', fontSize: '13px', color: '#666' }}>
                      <div><strong>Negative Score:</strong> {modalMatch.negativeScore.toFixed(2)}</div>
                      <div>Gradient Gap: {modalMatch.gradientAreaGap.toFixed(2)}</div>
                      <div>High Expression: {modalMatch.highExpressionArea.toFixed(2)}</div>
                    </div>
                  )}
                </div>
              </div>
            </CoordsProvider>

            {/* Navigation buttons */}
            <div style={{
              display: 'flex',
              justifyContent: 'center',
              gap: '10px',
              marginTop: '20px',
              paddingTop: '20px',
              borderTop: '1px solid #eee'
            }}>
              <button
                onClick={() => setModalMatchIndex(prev => (prev > 0 ? prev - 1 : prev))}
                disabled={modalMatchIndex <= 0}
                style={{
                  backgroundColor: modalMatchIndex <= 0 ? '#ccc' : '#1976d2',
                  color: '#fff',
                  border: 'none',
                  padding: '10px 20px',
                  borderRadius: '4px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: modalMatchIndex <= 0 ? 'not-allowed' : 'pointer',
                  transition: 'background-color 0.2s',
                  minWidth: '100px'
                }}
              >
                Previous
              </button>
              <button
                onClick={() => setModalMatchIndex(prev => (prev < cdMatches.length - 1 ? prev + 1 : prev))}
                disabled={modalMatchIndex >= cdMatches.length - 1}
                style={{
                  backgroundColor: modalMatchIndex >= cdMatches.length - 1 ? '#ccc' : '#1976d2',
                  color: '#fff',
                  border: 'none',
                  padding: '10px 20px',
                  borderRadius: '4px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: modalMatchIndex >= cdMatches.length - 1 ? 'not-allowed' : 'pointer',
                  transition: 'background-color 0.2s',
                  minWidth: '100px'
                }}
              >
                Next
              </button>
              <button
                onClick={() => setModalMatchIndex(null)}
                style={{
                  backgroundColor: '#1976d2',
                  color: '#fff',
                  border: 'none',
                  padding: '10px 20px',
                  borderRadius: '4px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'background-color 0.2s',
                  minWidth: '100px'
                }}
              >
                Done
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default NeuronDetail;
