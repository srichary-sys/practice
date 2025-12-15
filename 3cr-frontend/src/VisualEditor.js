import React, { useState, useEffect } from "react";
import ReactQuill from "react-quill";
import "react-quill/dist/quill.snow.css";

function VisualEditor({ transcriptText = "", onChange }) {
  const [content, setContent] = useState(transcriptText || "");

  // sync when prop changes (initial load and later changes)
  useEffect(() => {
    setContent(transcriptText || "");
  }, [transcriptText]);

  // propagate changes to parent if onChange provided
  const handleChange = (value) => {
    setContent(value);
    if (typeof onChange === "function") onChange(value);
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(content);
    alert("✅ HTML copied to clipboard!");
  };

  const handleSave = () => {
    const blob = new Blob([content], { type: "text/html" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "transcript.html";
    a.click();
    URL.revokeObjectURL(url);
  };

  const wordCount = content.replace(/<[^>]*>/g, "").split(/\s+/).filter(Boolean).length;
  const charCount = content.replace(/<[^>]*>/g, "").length;

  return (
    <div className="card shadow-sm">
      <div className="card-header bg-white fw-bold">Transcript Editor</div>
      <div className="card-body p-1">
        <ReactQuill
          theme="snow"
          value={content}
          onChange={handleChange}
          placeholder={transcriptText ? "Edit transcript..." : "No transcript yet — start typing here..."}
          style={{ height: "260px", marginBottom: "12px" }}
        />
      </div>
      <div className="card-footer bg-white d-flex justify-content-between align-items-center">
        <div>
          <button type="button" onClick={handleSave} className="btn btn-sm btn-primary me-2">
            💾 Save
          </button>
          <button type="button" onClick={handleCopy} className="btn btn-sm btn-secondary">
            📋 Copy
          </button>
        </div>
        <div className="text-muted small">
          Words: {wordCount} | Characters: {charCount}
        </div>
      </div>
    </div>
  );
}

export default VisualEditor;
