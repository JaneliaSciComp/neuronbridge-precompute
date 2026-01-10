import { useState, useEffect } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { isEM } from '../utils/neuronUtils'

function NeuronList() {
  const navigate = useNavigate();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);

  const [neurons, setNeurons] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(parseInt(searchParams.get('page') || '0', 10));
  const [totalPages, setTotalPages] = useState(0);
  const [filters, setFilters] = useState({
    libraryName: searchParams.get('libraryName') || '',
    alignmentSpace: searchParams.get('alignmentSpace') || '',
    publishedName: searchParams.get('publishedName') || '',
    mipId: searchParams.get('mipId') || ''
  });
  const [libraries, setLibraries] = useState([]);
  const [alignmentSpaces, setAlignmentSpaces] = useState([]);

  useEffect(() => {
    const alignmentSpaceParam = searchParams.get('alignmentSpace');
    fetchLibraries(alignmentSpaceParam);
    fetchAlignmentSpaces();
    // If there are URL params, fetch neurons on initial load
    const hasSearchParams = searchParams.get('libraryName') || searchParams.get('alignmentSpace') ||
                            searchParams.get('publishedName') || searchParams.get('mipId');
    if (hasSearchParams) {
      fetchNeurons();
    }
  }, []);

  useEffect(() => {
    if (neurons.length > 0) {
      fetchNeurons();
    }
  }, [page]);

  const fetchLibraries = async (alignmentSpace) => {
    try {
      const params = new URLSearchParams({
      });
      if (alignmentSpace && !filters.mipId) {
        params.append('alignmentSpace', alignmentSpace);
      }
      const response = await fetch(`/api/neurons/meta/libraries?${params}`);
      const data = await response.json();
      setLibraries(data);
    } catch (err) {
      console.error('Failed to fetch libraries:', err);
    }
  };

  const fetchAlignmentSpaces = async () => {
    try {
      const response = await fetch('/api/neurons/meta/alignment-spaces');
      const data = await response.json();
      setAlignmentSpaces(data);
    } catch (err) {
      console.error('Failed to fetch alignment spaces:', err);
    }
  };

  const fetchNeurons = async () => {
    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: '24'
      });

      if (filters.mipId) {
        params.append('mipId', filters.mipId);
      } else {
        if (filters.libraryName) params.append('libraryName', filters.libraryName);
        if (filters.alignmentSpace) params.append('alignmentSpace', filters.alignmentSpace);
        if (filters.publishedName) params.append('publishedName', filters.publishedName);
      }

      const response = await fetch(`/api/neurons?${params}`);
      if (!response.ok) throw new Error('Failed to fetch neurons');

      const data = await response.json();
      setNeurons(data.data);
      setTotalPages(data.pagination.totalPages);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const updateURLParams = (newFilters, newPage) => {
    const params = new URLSearchParams();

    if (newFilters.libraryName) params.set('libraryName', newFilters.libraryName);
    if (newFilters.alignmentSpace) params.set('alignmentSpace', newFilters.alignmentSpace);
    if (newFilters.publishedName) params.set('publishedName', newFilters.publishedName);
    if (newFilters.mipId) params.set('mipId', newFilters.mipId);
    if (newPage > 0) params.set('page', newPage.toString());

    navigate({ search: params.toString() }, { replace: true });
  };

  const handleFilterChange = (field, value) => {
    let newFilters;
    if (field === 'mipId') {
      // When setting MIP ID, clear library and alignment space filters
      newFilters = { ...filters, [field]: value, libraryName: '', alignmentSpace: '' };
      setFilters(newFilters);
    } else {
      newFilters = { ...filters, [field]: value };
      setFilters(newFilters);
    }

    if (field === 'alignmentSpace') {
      // trigger fetch libraries
      fetchLibraries(value);
   } else if (field == 'mipId') {
      fetchLibraries();
   }
  };

  const handleSearch = () => {
    setPage(0);
    updateURLParams(filters, 0);
    fetchNeurons();
  };

  const getAnatomicalArea = (space) => {
    switch (space) {
      case 'JRC2018_Unisex_20x_HR': return 'Brain';
      case 'JRC2018_VNC_Unisex_40x_DS': return 'VNC';
      default: return space;
    }
  };

  const getInputImageUrl = (neuron) => {
    // Try to get the source color depth image
    const cdInputImage = neuron.computeFiles?.InputColorDepthImage;

    if (cdInputImage) {
      return `/api/images/thumbnail?imagePath=${cdInputImage}`;
    }

    return null;
  };

  if (loading && neurons.length === 0) {
    return <div className="loading">Loading neurons...</div>;
  }

  return (
    <div>
      <div className="filters">
        <div className="filter-row">
          <div className="filter-group">
            <label>Published Name</label>
            <input
              type="text"
              placeholder="Search by name..."
              value={filters.publishedName}
              onChange={(e) => handleFilterChange('publishedName', e.target.value)}
            />
          </div>

          <div className="filter-group">
            <label>MIP ID</label>
            <input
              type="text"
              placeholder="Search by MIP ID..."
              value={filters.mipId}
              onChange={(e) => handleFilterChange('mipId', e.target.value)}
            />
          </div>

          <div className="filter-group">
            <label>Library</label>
            <select
              value={filters.libraryName}
              onChange={(e) => handleFilterChange('libraryName', e.target.value)}
              disabled={!!filters.mipId}
              style={{
                minWidth: '330px',
                opacity: filters.mipId ? 0.5 : 1,
                cursor: filters.mipId ? 'not-allowed' : 'pointer'
              }}
            >
              <option value="">All Libraries</option>
              {libraries.map(lib => (
                <option key={lib} value={lib}>{lib}</option>
              ))}
            </select>
          </div>

          <div className="filter-group">
            <label>Anatomical Area</label>
            <select
              value={filters.alignmentSpace}
              onChange={(e) => handleFilterChange('alignmentSpace', e.target.value)}
              disabled={!!filters.mipId}
              style={{
                minWidth: '200px',
                opacity: filters.mipId ? 0.5 : 1,
                cursor: filters.mipId ? 'not-allowed' : 'pointer'
              }}
            >
              <option value="">All</option>
              {alignmentSpaces.map(space => (
                <option key={space} value={space}>
                  {getAnatomicalArea(space)}
                </option>
              ))}
            </select>
          </div>

          <div className="filter-group">
            <label>&nbsp;</label>
            <button
              onClick={handleSearch}
              style={{
                backgroundColor: '#1976d2',
                color: '#fff',
                border: 'none',
                padding: '10px 24px',
                borderRadius: '4px',
                fontSize: '14px',
                fontWeight: '500',
                cursor: 'pointer'
              }}
            >
              Search
            </button>
          </div>
        </div>
      </div>

      {error && <div className="error">Error: {error}</div>}

      <div className="neuron-grid">
        {neurons.map(neuron => (
          <Link
            to={`/neuron/${neuron._id}`}
            key={neuron._id}
            className="neuron-card"
            state={{ fromSearch: location.search }}
          >
            {getInputImageUrl(neuron) && (
              <img
                src={getInputImageUrl(neuron)}
                alt={neuron.publishedName}
                className="neuron-image"
              />
            )}
            <h3>{neuron.publishedName}</h3>
            <p><strong>Library:</strong> {neuron.libraryName}</p>
            <p><strong>Alignment:</strong> {neuron.alignmentSpace}</p>
            {isEM(neuron)
              ? (
                <p><strong>Body ID:</strong> {neuron.publishedName}</p>
              )
              : (
                <p><strong>Slide Code:</strong> {neuron.slideCode}</p>
              )
            }
            {neuron.mipId && <p><strong>MIP ID:</strong> {neuron.mipId}</p>}
          </Link>
        ))}
      </div>

      {neurons.length === 0 && !loading && (
        <div style={{ textAlign: 'center', padding: '40px', color: '#666' }}>
          No neurons found. Click "Search" to find neurons or try adjusting your filters.
        </div>
      )}

      {neurons.length > 0 && (
        <div className="pagination">
          <button
            onClick={() => {
              const newPage = Math.max(0, page - 1);
              setPage(newPage);
              updateURLParams(filters, newPage);
            }}
            disabled={page === 0}
          >
            Previous
          </button>
          <span>Page {page + 1} of {totalPages}</span>
          <button
            onClick={() => {
              const newPage = Math.min(totalPages - 1, page + 1);
              setPage(newPage);
              updateURLParams(filters, newPage);
            }}
            disabled={page >= totalPages - 1}
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}

export default NeuronList;
