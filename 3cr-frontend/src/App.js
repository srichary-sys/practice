import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import "bootstrap/dist/css/bootstrap.min.css";
import "./App.css";
import VisualEditor from './VisualEditor'; // Import the VisualEditor

const API_BASE = "http://127.0.0.1:8000/api/videos";
const FILTER_COUNT_API = "http://127.0.0.1:8000/api/videos/filter_counts";
const PAGE_LIMIT = 1000;

const App = () => {
  const [videos, setVideos] = useState([]);
  const [loading, setLoading] = useState(false);
  const [nextCursor, setNextCursor] = useState(null);
  const [searchText, setSearchText] = useState("");
  const [searchField, setSearchField] = useState("title");
  const [filters, setFilters] = useState({
    title: "",
    description: "",
    transcriptText: "",
  });
  const [estimatedCount, setEstimatedCount] = useState(0);
  const [filterCounts, setFilterCounts] = useState({
    titleDocs: 0,
    descDocs: 0,
    transDocs: 0,
    totalDocs: 0,
  });
  const [tempSearchText, setTempSearchText] = useState(searchText);
  const [showVideoModal, setShowVideoModal] = useState(false);
  const [videoToShow, setVideoToShow] = useState(null);
  const [showAddModal, setShowAddModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [videoToEdit, setVideoToEdit] = useState(null);
  const [appliedFilters, setAppliedFilters] = useState([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [darkMode, setDarkMode] = useState(false);

   // -------------------------------- Fetch Filter Counts -----------------------------
  const fetchFilterCounts = useCallback(async () => {
    if (!searchText.trim()) {
      setFilterCounts({
        titleDocs: 0,
        descDocs: 0,
        transDocs: 0,
        totalDocs: 0,
      });
      return;
    }

    try {
      const res = await axios.get(FILTER_COUNT_API, {
        params: { search: searchText },
      });
      setFilterCounts(res.data);
    } catch (err) {
      console.error("Filter Count Error:", err);
    }
  }, [searchText]);


  // ---------------- Fetch Videos ----------------
  const fetchVideos = useCallback(async () => {
    setLoading(true);
    try {
      const params = {
        limit: PAGE_LIMIT,
        title: filters.title,
        description: filters.description,
        transcriptText: filters.transcriptText,
        search: searchText,
        search_field: searchField,
      };

      const res = await axios.get(API_BASE, { params });
      const newVideos = res.data.data || [];
      const { filter_counts, estimated_count, next_cursor } = res.data;

      setVideos(newVideos);
      setNextCursor(next_cursor || null);
      setFilterCounts(filter_counts || {});
      setEstimatedCount(estimated_count);
      setTotalPages(Math.ceil(estimated_count / PAGE_LIMIT));
    } catch (err) {
      console.error("Error fetching videos:", err);
    } finally {
      setLoading(false);
    }
  }, [filters, searchText, searchField]);

  useEffect(() => {
    fetchVideos();
  }, [fetchVideos]);

   // Fetch filter counts when search text updates
  useEffect(() => {
    fetchFilterCounts();
  }, [fetchFilterCounts]);

   // -------------------------------- Filter Logic -------------------------------------
  const handleFilterChange = (field, value) => {
    setFilters((prev) => ({ ...prev, [field]: value }));

    const updatedFilters = appliedFilters.filter((f) => f.field !== field);

    if (value.trim()) {
      updatedFilters.push({ field, value });
    }

    setAppliedFilters(updatedFilters);
  };

  const getFilteredVideos = () => {
    let filtered = [...videos];
    const text = searchText.trim().toLowerCase();

    if (text) {
      if (searchField === "search") {
        filtered = filtered.filter(
          (v) =>
            v.title?.toLowerCase().includes(text) ||
            v.description?.toLowerCase().includes(text) ||
            v.transcriptText?.toLowerCase().includes(text)
        );
      } else {
        filtered = filtered.filter((v) =>
          v[searchField]?.toLowerCase().includes(text)
        );
      }
    } else {
      Object.entries(filters).forEach(([key, val]) => {
        if (val.trim()) {
          filtered = filtered.filter((v) =>
            v[key]?.toLowerCase().includes(val.trim().toLowerCase())
          );
        }
      });
    }

    return filtered;
  };

  const filteredVideos = getFilteredVideos();
  

  const handleSearch = () => {
    setSearchText(tempSearchText);
    fetchVideos();
    fetchFilterCounts();
  };

  // ---------------- Pagination ----------------
  const handlePageChange = async (targetPage) => {
    if (targetPage < 1 || targetPage > totalPages || targetPage === page) return;

    setPage(targetPage);
    setLoading(true);

    const requiredDataCount = targetPage * PAGE_LIMIT;
    let cursor = nextCursor;

    try {
      while (filteredVideos.length < requiredDataCount && cursor) {
        const params = {
          limit: PAGE_LIMIT,
          last_id: cursor, // Using cursor for pagination
          title: filters.title,
          description: filters.description,
          transcriptText: filters.transcriptText,
          search: searchText,
          search_field: searchField,
        };

        const res = await axios.get(API_BASE, { params });
        const newVideos = res.data.data || [];
        cursor = res.data.next_cursor || null;

        filteredVideos.push(...newVideos); // Append new videos
      }

      setVideos(filteredVideos);
      setNextCursor(cursor);
    } catch (err) {
      console.error("Error loading page data:", err);
    } finally {
      setLoading(false);
      window.scrollTo({ top: 300, behavior: "smooth" }); // Scroll for user convenience
    }
  };

  const handleDelete = async (id) => {
    if (!window.confirm("Are you sure to delete this video?")) return;

    try {
      await axios.delete(`${API_BASE}/${id}`);
      setVideos((prev) => prev.filter((v) => v._id !== id));
    } catch (err) {
      console.error("Delete error:", err);
    }
  };

  const hasFilters =
    Object.values(filters).some((value) => value.trim() !== "") ||
    searchText.trim() !== "";

const getMatchedDocumentCount = () => {
  // If user is typing in search bar
  if (searchText.trim()) {
    switch (searchField) {
      case "title":
        return filterCounts.titleDocs || 0;
      case "description":
        return filterCounts.descDocs || 0;
      case "transcriptText":
        return filterCounts.transDocs || 0;
      case "search": // Any Field
        return filterCounts.totalDocs || 0;
      default:
        return filterCounts.totalDocs || 0;
    }
  }

  // If user is using filter boxes instead of search bar
  if (filters.title.trim()) return filterCounts.titleDocs || 0;
  if (filters.description.trim()) return filterCounts.descDocs || 0;
  if (filters.transcriptText.trim()) return filterCounts.transDocs || 0;

  // No filters → return total estimated count
  return estimatedCount;
};


  // ---------------- Toggle Dark Mode ----------------
  const toggleDarkMode = () => {
    setDarkMode((prev) => !prev);
  };

  useEffect(() => {
    if (darkMode) {
      document.body.classList.add("dark-mode");
    } else {
      document.body.classList.remove("dark-mode");
    }
  }, [darkMode]);

  return (
    <div className={`container mt-4 ${darkMode ? 'dark-mode' : ''}`}>
      <h2 className="text-center mb-4">🎬 Video Dashboard 🧑‍💻 </h2>

      {/* Dark Mode Toggle Button */}
      <div className="text-end mb-3">
        <button className="btn btn-secondary" onClick={toggleDarkMode}>
          {darkMode ? " 🌞 " : " 🌙 "}
        </button>
      </div>

      {/* Search */}
      <div className="card p-3 mb-3">
        <div className="row g-2">
          <div className="col-md-3">
            <select
              className="form-select"
              value={searchField}
              onChange={(e) => setSearchField(e.target.value)}
            >
              <option value="title">Title</option>
              <option value="description">Description</option>
              <option value="transcriptText">Transcript Text</option>
              <option value="search">Any Field</option>
            </select>
          </div>

          <div className="col-md-6">
            <input
              type="text"
              className="form-control"
              placeholder="🔍 Search..."
              value={tempSearchText}
              onChange={(e) => setTempSearchText(e.target.value)}
            />
          </div>

          <div className="col-md-3">
            <button className="btn btn-primary w-100" onClick={handleSearch}>
              Apply
            </button>
          </div>
        </div>

        <div className="mt-3 d-flex justify-content-end">
          <button
            className="btn btn-secondary"
            onClick={() => {
              setTempSearchText(""); // Reset the temporary search text
              setSearchText(""); // Reset the actual search text
              setFilters({ title: "", description: "", transcriptText: "" });
              setAppliedFilters([]); // Reset applied filters
              setPage(1); // Reset to page 1
              setEstimatedCount(videos.length); // Reset matched documents count
            }}
          >
            Reset
          </button>
        </div>
      </div>
      
      {/* ---------------- FILTER COUNT CARDS (Option B) ---------------- */}
      {searchText && (
        <div className="row mb-4 text-center">
          <div className="col-md-3">
            <div className="card shadow-sm p-3">
              <h5>Title Matches</h5>
              <h3>{filterCounts.titleDocs}</h3>
            </div>
          </div>
          <div className="col-md-3">
            <div className="card shadow-sm p-3">
              <h5>Description Matches</h5>
              <h3>{filterCounts.descDocs}</h3>
            </div>
          </div>
          <div className="col-md-3">
            <div className="card shadow-sm p-3">
              <h5>Transcript Matches</h5>
              <h3>{filterCounts.transDocs}</h3>
            </div>
          </div>
          <div className="col-md-3">
            <div className="card shadow-sm p-3">
              <h5>Total Matches</h5>
              <h3>{filterCounts.totalDocs}</h3>
            </div>
          </div>
        </div>
      )}

      {/* ADD BUTTON */}
      <button className="btn btn-success mb-3" onClick={() => setShowAddModal(true)}>
        ➕ Add New Video
      </button>

      <div className="mb-2 text-center">
        <p>Total Matched Documents: {getMatchedDocumentCount()}</p>
      </div>

      {/*<div className="mb-3 text-center">
        <p>
          Showing{" "}
          {filteredVideos.length > 0 ? (page - 1) * PAGE_LIMIT + 1 : 0} -{" "}
          {Math.min(page * PAGE_LIMIT, filteredVideos.length)}
        </p>
      </div>*/}

      {/* Pagination */}
      <div className="d-flex justify-content-center mb-3 align-items-center">
        <button
          className="btn btn-outline-primary btn-sm me-2"
          disabled={page === 1}
          onClick={() => handlePageChange(page - 1)}
        >
          Previous
        </button>

        {/* Pagination Buttons */}
        {(() => {
          const buttons = [];
          const maxVisible = 7;
          let startPage = Math.max(2, page - 2);
          let endPage = Math.min(totalPages - 1, page + 2);

          if (page <= 4) endPage = Math.min(maxVisible - 1, totalPages - 1);
          if (page >= totalPages - 3)
            startPage = Math.max(totalPages - (maxVisible - 2), 2);

          buttons.push(
            <button
              key={1}
              className={`btn btn-outline-primary btn-sm mx-1 ${page === 1 ? "active" : ""}`}
              onClick={() => handlePageChange(1)}
            >
              1
            </button>
          );

          if (startPage > 2)
            buttons.push(
              <span key="start-ellipsis" className="mx-1">...</span>
            );

          for (let p = startPage; p <= endPage; p++) {
            buttons.push(
              <button
                key={p}
                className={`btn btn-outline-primary btn-sm mx-1 ${p === page ? "active" : ""}`}
                onClick={() => handlePageChange(p)}
              >
                {p}
              </button>
            );
          }

          if (endPage < totalPages - 1)
            buttons.push(
              <span key="end-ellipsis" className="mx-1">...</span>
            );

          if (totalPages > 1) {
            buttons.push(
              <button
                key={totalPages}
                className={`btn btn-outline-primary btn-sm mx-1 ${page === totalPages ? "active" : ""}`}
                onClick={() => handlePageChange(totalPages)}
              >
                {totalPages}
              </button>
            );
          }

          return buttons;
        })()}

        <button
          className="btn btn-outline-primary btn-sm ms-2"
          disabled={page === totalPages}
          onClick={() => handlePageChange(page + 1)}
        >
          Next
        </button>
      </div>

      {/* Table */}
      <table className="table table-striped table-hover">
        <thead>
          <tr>
            <th>SI.No</th>
            <th>ID</th>
            <th>Title</th>
            <th>Views</th>
            <th>Published At</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {loading ? (
            <tr>
              <td colSpan="6" className="text-center">
                Loading...
              </td>
            </tr>
          ) : filteredVideos.length > 0 ? (
            filteredVideos
              .slice((page - 1) * PAGE_LIMIT, page * PAGE_LIMIT)
              .map((v, idx) => (
                <tr key={v._id}>
                  <td>{(page - 1) * PAGE_LIMIT + idx + 1}</td>
                  <td>{v.Id}</td>
                  <td>
                    <button
                      className="btn btn-link p-0"
                      onClick={() => {
                        setVideoToShow(v);
                        setShowVideoModal(true);
                      }}
                    >
                      {v.title}
                    </button>
                  </td>
                  <td>{v.viewCount}</td>
                  <td>{v.publishedAt}</td>
                  <td>
                    <button
                      className="btn btn-warning btn-sm me-2"
                      onClick={() => {
                        setVideoToEdit(v);
                        setShowEditModal(true);
                      }}
                    >
                      Edit
                    </button>
                    <button
                      className="btn btn-danger btn-sm"
                      onClick={() => handleDelete(v._id)}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))
          ) : (
            <tr>
              <td colSpan="6" className="text-center">
                No videos found.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {/* Modals */}
      {showVideoModal && videoToShow && (
        <VideoModal
          video={videoToShow}
          closeModal={() => setShowVideoModal(false)}
        />
      )}

      <AddEditModal
        show={showAddModal}
        close={() => setShowAddModal(false)}
        setVideos={setVideos}
      />

      {videoToEdit && (
        <AddEditModal
          show={showEditModal}
          close={() => setShowEditModal(false)}
          setVideos={setVideos}
          video={videoToEdit}
        />
      )}
    </div>
  );
};

// Video Modal
const VideoModal = ({ video, closeModal }) => {
  const getVideoId = (url) => {
    if (!url) return null;
    if (url.includes("youtu.be")) return url.split("/").pop();
    try {
      return new URL(url).searchParams.get("v");
    } catch {
      return null;
    }
  };

  return (
    <div className="modal fade show d-block" style={{ background: "rgba(0,0,0,.5)" }}>
      <div className="modal-dialog modal-lg">
        <div className="modal-content">
          <div className="modal-header">
            <h5>{video.title}</h5>
            <button className="btn-close" onClick={closeModal}></button>
          </div>
          <div className="modal-body">
            <div className="ratio ratio-16x9">
              {getVideoId(video.sourceUrl) ? (
                <iframe
                  src={`https://www.youtube.com/embed/${getVideoId(video.sourceUrl)}`}
                  title={video.title}
                  frameBorder="0"
                  allowFullScreen
                ></iframe>
              ) : (
                <p>No video URL found</p>
              )}
            </div>
          </div>
          <div className="modal-footer">
            <button className="btn btn-secondary" onClick={closeModal}>
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

// Add/Edit Modal
const AddEditModal = ({ show, close, setVideos, video }) => {
  const FIELDS = {
    Id: "",
    title: "",
    description: "",
    publishedAt: "",
    viewCount: "",
    sourceUrl: "",
    transcriptText: "",
  };

  const [formData, setFormData] = useState(FIELDS);

  useEffect(() => {
    if (video) {
      const clean = {};
      Object.keys(FIELDS).forEach((key) => {
        clean[key] = video[key] ?? "";
      });
      setFormData(clean);
    } else {
      setFormData({ ...FIELDS });
    }
  }, [video]);

  const handleChange = (e) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  const handleEditorChange = (value) => {
    setFormData((prev) => ({ ...prev, transcriptText: value }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if (video) {
        await axios.put(`${API_BASE}/${video._id}`, formData);
        setVideos((prev) =>
          prev.map((item) =>
            item._id === video._id ? { ...item, ...formData } : item
          )
        );
      } else {
        const res = await axios.post(API_BASE, formData);
        setVideos((prev) => [res.data, ...prev]);
      }
      close();
    } catch (err) {
      console.error("Save error:", err);
      alert("Failed to save item");
    }
  };

  if (!show) return null;

  return (
    <div className="modal fade show d-block" style={{ background: "rgba(0,0,0,.5)" }}>
      <div className="modal-dialog modal-lg">
        <div className="modal-content">
          <div className="modal-header">
            <h5>{video ? "Edit Video" : "Add Video"}</h5>
            <button className="btn-close" onClick={close}></button>
          </div>
          <div className="modal-body">
            <form onSubmit={handleSubmit}>
              {Object.keys(FIELDS).map((key) => {
                if (key === 'transcriptText') {
                  return (
                    <div className="mb-3" key={key}>
                      <label className="form-label">{key}</label>
                      <VisualEditor 
                        transcriptText={formData.transcriptText} 
                        onChange={handleEditorChange} 
                      />
                    </div>
                  );
                }
                return (
                  <div className="mb-3" key={key}>
                    <label className="form-label">{key}</label>
                    <input
                      type="text"
                      name={key}
                      className="form-control"
                      value={formData[key]}
                      onChange={handleChange}
                    />
                  </div>
                );
              })}
              <button className="btn btn-primary" type="submit">
                {video ? "Update" : "Add"} Video
              </button>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};

export default App;
