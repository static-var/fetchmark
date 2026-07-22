package eval

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
)

const maxLabelUIDataBytes = 32 << 20

type labelUIPayload struct {
	SchemaVersion int                  `json:"schema_version"`
	RunID         string               `json:"run_id"`
	Rows          []labelTemplateEntry `json:"rows"`
}

// WriteLabelUI emits a self-contained offline workbench for blind relevance
// grading. The encoded payload deliberately contains only label-template
// fields; provider identity and result provenance never enter the document.
func WriteLabelUI(writer io.Writer, records []Record) error {
	if writer == nil {
		return errors.New("eval: nil label-UI writer")
	}
	runID, _, err := indexRunResults(records)
	if err != nil {
		return err
	}
	if err := validateLabelTemplateBudget(records, maxEvaluationArtifactLineSize, maxEvaluationArtifactBytes); err != nil {
		return err
	}
	rows := make([]labelTemplateEntry, 0)
	for _, record := range records {
		for index, result := range record.Results {
			rows = append(rows, makeLabelTemplateEntry(record, result, index+1))
		}
	}
	if len(rows) == 0 {
		return errors.New("eval: label UI requires at least one result")
	}
	raw, err := json.Marshal(labelUIPayload{SchemaVersion: 1, RunID: runID, Rows: rows})
	if err != nil {
		return fmt.Errorf("eval: encode label UI payload: %w", err)
	}
	if len(raw) > maxLabelUIDataBytes {
		return fmt.Errorf("eval: label UI payload exceeds %d bytes", maxLabelUIDataBytes)
	}
	data := struct {
		Payload string
	}{Payload: base64.StdEncoding.EncodeToString(raw)}
	if err := labelUIPage.Execute(writer, data); err != nil {
		return fmt.Errorf("eval: write label UI: %w", err)
	}
	return nil
}

var labelUIPage = template.Must(template.New("label-ui").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src 'none'; connect-src 'none'; font-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'">
  <title>Fetchmark relevance calibration</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #182631;
      --muted: #5b6d79;
      --line: #bdcbd3;
      --panel: #f8fbfc;
      --ground: #dfe9ed;
      --blue: #255f86;
      --blue-soft: #d7e9f3;
      --amber: #a65f16;
      --green: #28705e;
      --red: #a54438;
      --shadow: 0 18px 50px rgba(24, 38, 49, .12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      background:
        linear-gradient(rgba(37, 95, 134, .05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(37, 95, 134, .05) 1px, transparent 1px),
        var(--ground);
      background-size: 28px 28px;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button, input { font: inherit; }
    button, a { -webkit-tap-highlight-color: transparent; }
    button:focus-visible, a:focus-visible, input:focus-visible {
      outline: 3px solid #e9a23b;
      outline-offset: 3px;
    }
    .skip-link {
      position: fixed;
      z-index: 10;
      top: 8px;
      left: 8px;
      padding: 10px 13px;
      color: #fff;
      background: var(--blue);
      transform: translateY(-160%);
    }
    .skip-link:focus { transform: translateY(0); }
    .sr-only {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }
    .shell {
      width: min(1080px, calc(100% - 32px));
      margin: 32px auto;
      background: var(--panel);
      border: 1px solid #aebdc6;
      box-shadow: var(--shadow);
    }
    .masthead {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 24px;
      padding: 24px 28px 20px;
      border-bottom: 1px solid var(--line);
      background: #edf4f7;
    }
    .eyebrow, .utility, .meta, .shortcut, .run {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      letter-spacing: .04em;
    }
    .eyebrow {
      margin: 0 0 8px;
      color: var(--blue);
      font-size: 12px;
      font-weight: 750;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      font-family: Charter, "Iowan Old Style", Georgia, serif;
      font-size: clamp(27px, 4vw, 44px);
      font-weight: 600;
      line-height: 1.02;
      letter-spacing: -.025em;
    }
    .run {
      margin-top: 10px;
      color: var(--muted);
      font-size: 11px;
      overflow-wrap: anywhere;
    }
    .progress-block { min-width: 190px; align-self: end; }
    .progress-copy {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 7px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .progress-track { height: 8px; overflow: hidden; background: #cbd8de; }
    .progress-fill { width: 0; height: 100%; background: var(--green); transition: width 120ms ease-out; }
    @media (prefers-reduced-motion: reduce) { .progress-fill { transition: none; } }
    .rail-wrap { padding: 14px 28px; border-bottom: 1px solid var(--line); }
    .rail {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(14px, 1fr));
      gap: 4px;
      max-height: 72px;
      overflow-y: auto;
    }
    .rail button {
      min-width: 14px;
      height: 14px;
      padding: 0;
      border: 1px solid #9babb4;
      background: #fff;
      cursor: pointer;
    }
    .rail button.current { outline: 2px solid var(--ink); outline-offset: 1px; }
    .rail button.g0 { background: #cf7167; border-color: var(--red); }
    .rail button.g1 { background: #e2a55b; border-color: var(--amber); }
    .rail button.g2 { background: #77a7c2; border-color: var(--blue); }
    .rail button.g3 { background: #5b9a88; border-color: var(--green); }
    .workspace { padding: clamp(22px, 4vw, 48px); }
    .position {
      display: flex;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 26px;
      color: var(--muted);
      font-size: 12px;
    }
    .query {
      max-width: 880px;
      margin: 0 0 32px;
      font-family: Charter, "Iowan Old Style", Georgia, serif;
      font-size: clamp(22px, 3.2vw, 34px);
      font-weight: 500;
      line-height: 1.2;
      letter-spacing: -.015em;
    }
    .result-card {
      padding: 22px;
      border-left: 5px solid var(--blue);
      background: #fff;
      box-shadow: 0 8px 24px rgba(24, 38, 49, .07);
    }
    .result-title { margin: 0 0 10px; font-size: 20px; line-height: 1.3; }
    .result-link { color: var(--blue); overflow-wrap: anywhere; }
    .meta { margin-top: 14px; color: var(--muted); font-size: 11px; }
    .grade-heading { margin: 34px 0 12px; font-size: 13px; }
    .grades { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    .grade {
      min-height: 98px;
      padding: 14px;
      text-align: left;
      color: var(--ink);
      border: 1px solid var(--line);
      background: #edf3f5;
      cursor: pointer;
    }
    .grade:hover { border-color: var(--blue); background: var(--blue-soft); }
    .grade[aria-pressed="true"] { border: 2px solid var(--blue); background: var(--blue-soft); }
    .grade-number { display: block; margin-bottom: 9px; font: 750 22px/1 ui-monospace, monospace; }
    .grade-name { display: block; font-weight: 750; }
    .grade-note { display: block; margin-top: 4px; color: var(--muted); font-size: 12px; line-height: 1.35; }
    .controls, .exports {
      display: flex;
      flex-wrap: wrap;
      gap: 9px;
      align-items: center;
    }
    .controls { justify-content: space-between; margin-top: 28px; }
    .control-button, .export-button, .import-label {
      padding: 10px 13px;
      border: 1px solid #8fa2ad;
      background: #fff;
      color: var(--ink);
      cursor: pointer;
    }
    .control-button.primary, .export-button.primary { color: #fff; border-color: var(--blue); background: var(--blue); }
    button:disabled { cursor: not-allowed; opacity: .45; }
    .footer {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 20px;
      padding: 18px 28px;
      border-top: 1px solid var(--line);
      background: #edf4f7;
    }
    .status { margin: 0; color: var(--muted); font-size: 12px; align-self: center; }
    .shortcut { color: var(--muted); font-size: 10px; }
    .file-input { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); }
    @media (max-width: 760px) {
      .shell { width: 100%; margin: 0; border-width: 0; }
      .masthead, .footer { grid-template-columns: 1fr; }
      .progress-block { min-width: 0; }
      .grades { grid-template-columns: 1fr 1fr; }
      .workspace, .masthead, .rail-wrap, .footer { padding-left: 18px; padding-right: 18px; }
    }
  </style>
</head>
<body>
  <a class="skip-link" href="#workspace">Skip result navigator</a>
  <main class="shell" data-label-payload="{{.Payload}}">
    <header class="masthead">
      <div>
        <p class="eyebrow">Fetchmark evaluation instrument</p>
        <h1>Relevance calibration</h1>
        <div class="run" id="run-id"></div>
      </div>
      <div class="progress-block" aria-live="polite">
        <div class="progress-copy"><span>Judgments</span><span id="progress-copy">0 / 0</span></div>
        <div class="progress-track"><div class="progress-fill" id="progress-fill"></div></div>
      </div>
    </header>

    <section class="rail-wrap" aria-label="Result evidence rail">
      <div class="rail" id="rail"></div>
    </section>

    <section class="workspace" id="workspace" tabindex="-1">
      <div class="position utility"><span id="position"></span><span id="case-position"></span></div>
      <p class="sr-only" id="result-announcement" aria-live="polite" aria-atomic="true"></p>
      <h2 class="query" id="query"></h2>
      <article class="result-card">
        <h3 class="result-title" id="title"></h3>
        <a class="result-link" id="result-link" target="_blank" rel="noopener noreferrer"></a>
        <div class="meta" id="meta"></div>
      </article>

      <h3 class="grade-heading">How useful is this result for the query?</h3>
      <div class="grades" id="grades">
        <button class="grade" type="button" data-grade="0"><span class="grade-number">0</span><span class="grade-name">Irrelevant</span><span class="grade-note">Does not help answer the query.</span></button>
        <button class="grade" type="button" data-grade="1"><span class="grade-number">1</span><span class="grade-name">Marginal</span><span class="grade-note">Related, but not useful enough.</span></button>
        <button class="grade" type="button" data-grade="2"><span class="grade-number">2</span><span class="grade-name">Relevant</span><span class="grade-note">A useful answer or source.</span></button>
        <button class="grade" type="button" data-grade="3"><span class="grade-number">3</span><span class="grade-name">Highly relevant</span><span class="grade-note">Directly answers the query.</span></button>
      </div>

      <div class="controls">
        <div class="controls">
          <button class="control-button" type="button" id="previous">Previous</button>
          <button class="control-button" type="button" id="next">Next</button>
        </div>
        <button class="control-button primary" type="button" id="next-ungraded">Next ungraded</button>
      </div>
      <p class="shortcut">Keyboard: 0–3 grade · ←/→ move · U next ungraded</p>
    </section>

    <footer class="footer">
      <p class="status" id="status" aria-live="polite">Grades stay in this browser when storage is available. Export a draft before closing.</p>
      <div class="exports">
        <button class="export-button" type="button" id="export-draft">Export draft</button>
        <label class="import-label" for="import-draft">Import draft</label>
        <input class="file-input" id="import-draft" type="file" accept=".jsonl,application/json,text/plain">
        <button class="export-button primary" type="button" id="export-complete" disabled>Export completed labels</button>
      </div>
    </footer>
  </main>

  <script>
    "use strict";
    const root = document.querySelector("[data-label-payload]");
    const binary = atob(root.dataset.labelPayload);
    const payloadBytes = Uint8Array.from(binary, character => character.charCodeAt(0));
    const data = JSON.parse(new TextDecoder().decode(payloadBytes));
    const rows = data.rows;
    const grades = new Array(rows.length).fill(null);
    const bindings = rows.map(row => [
      row.schema_version,
      row.run_id,
      row.case_id,
      row.intent,
      row.query,
      row.case_sha256,
      row.url,
      row.title || "",
      row.domain || "",
      row.rank
    ]);
    const storageKey = "fetchmark-labels:" + data.run_id + ":" + rows.length;
    let current = 0;

    const elements = {
      runID: document.getElementById("run-id"),
      progressCopy: document.getElementById("progress-copy"),
      progressFill: document.getElementById("progress-fill"),
      rail: document.getElementById("rail"),
      position: document.getElementById("position"),
      casePosition: document.getElementById("case-position"),
      query: document.getElementById("query"),
      title: document.getElementById("title"),
      link: document.getElementById("result-link"),
      meta: document.getElementById("meta"),
      status: document.getElementById("status"),
      announcement: document.getElementById("result-announcement"),
      complete: document.getElementById("export-complete")
    };

    function validGrade(value) {
      return value === null || (Number.isInteger(value) && value >= 0 && value <= 3);
    }

    function persist() {
      try {
        localStorage.setItem(storageKey, JSON.stringify({bindings, grades}));
      } catch (_) {
        elements.status.textContent = "Browser storage is unavailable. Export a draft before closing.";
      }
    }

    function restore() {
      try {
        const raw = localStorage.getItem(storageKey);
        if (!raw) return;
        const saved = JSON.parse(raw);
        if (!Array.isArray(saved.bindings) || saved.bindings.length !== rows.length || !Array.isArray(saved.grades) || saved.grades.length !== rows.length) return;
        const exact = bindings.every((binding, index) =>
          Array.isArray(saved.bindings[index]) &&
          saved.bindings[index].length === binding.length &&
          binding.every((value, fieldIndex) => saved.bindings[index][fieldIndex] === value));
        if (!exact || !saved.grades.every(validGrade)) return;
        saved.grades.forEach((grade, index) => grades[index] = grade);
      } catch (_) {
        elements.status.textContent = "Saved browser progress could not be restored. Import a draft instead.";
      }
    }

    function gradedCount() {
      return grades.reduce((count, grade) => count + (grade === null ? 0 : 1), 0);
    }

    function createRail() {
      const fragment = document.createDocumentFragment();
      rows.forEach((row, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.tabIndex = index === current ? 0 : -1;
        button.title = row.case_id + " · rank " + row.rank;
        button.setAttribute("aria-label", "Open judgment " + (index + 1));
        button.addEventListener("click", () => { current = index; render(); });
        button.addEventListener("keydown", event => {
          let destination = -1;
          if (event.key === "ArrowLeft" || event.key === "ArrowUp") destination = Math.max(0, current - 1);
          else if (event.key === "ArrowRight" || event.key === "ArrowDown") destination = Math.min(rows.length - 1, current + 1);
          else if (event.key === "Home") destination = 0;
          else if (event.key === "End") destination = rows.length - 1;
          if (destination < 0) return;
          event.preventDefault();
          event.stopPropagation();
          current = destination;
          render();
          elements.rail.children[current].focus();
        });
        fragment.appendChild(button);
      });
      elements.rail.appendChild(fragment);
    }

    function updateProgress() {
      const count = gradedCount();
      elements.progressCopy.textContent = count + " / " + rows.length;
      elements.progressFill.style.width = ((count / rows.length) * 100) + "%";
      elements.complete.disabled = count !== rows.length;
      Array.from(elements.rail.children).forEach((cell, index) => {
        cell.tabIndex = index === current ? 0 : -1;
        cell.className = (index === current ? "current " : "") + (grades[index] === null ? "" : "g" + grades[index]);
        cell.setAttribute("aria-label", "Open judgment " + (index + 1) + (grades[index] === null ? ", ungraded" : ", grade " + grades[index]));
      });
    }

    function render() {
      const row = rows[current];
      elements.runID.textContent = "Run " + data.run_id;
      elements.position.textContent = "Result " + (current + 1) + " of " + rows.length;
      elements.casePosition.textContent = row.case_id + " · rank " + row.rank;
      elements.query.textContent = row.query;
      elements.title.textContent = row.title || "Untitled result";
      elements.announcement.textContent = "Result " + (current + 1) + " of " + rows.length + ". Query: " + row.query + ". Result: " + (row.title || "Untitled result") + ".";
      elements.link.textContent = row.url;
      elements.link.href = row.url;
      elements.meta.textContent = row.domain + " · " + row.intent;
      document.querySelectorAll("[data-grade]").forEach(button => {
        button.setAttribute("aria-pressed", Number(button.dataset.grade) === grades[current] ? "true" : "false");
      });
      document.getElementById("previous").disabled = current === 0;
      document.getElementById("next").disabled = current === rows.length - 1;
      updateProgress();
    }

    function nextUngraded(from) {
      for (let offset = 1; offset <= rows.length; offset++) {
        const candidate = (from + offset) % rows.length;
        if (grades[candidate] === null) return candidate;
      }
      return -1;
    }

    function setGrade(grade) {
      grades[current] = grade;
      persist();
      const next = nextUngraded(current);
      if (next >= 0) current = next;
      elements.status.textContent = next < 0 ? "All judgments are complete. Export completed labels." : "Grade saved in this browser.";
      render();
    }

    function labelDocument(row, relevance) {
      return {
        schema_version: row.schema_version,
        run_id: row.run_id,
        case_id: row.case_id,
        intent: row.intent,
        query: row.query,
        case_sha256: row.case_sha256,
        url: row.url,
        ...(row.title ? {title: row.title} : {}),
        ...(row.domain ? {domain: row.domain} : {}),
        rank: row.rank,
        relevance
      };
    }

    function downloadLabels(complete) {
      if (complete && gradedCount() !== rows.length) return;
      const text = rows.map((row, index) => JSON.stringify(labelDocument(row, grades[index]))).join("\n") + "\n";
      const blobURL = URL.createObjectURL(new Blob([text], {type: "application/x-ndjson;charset=utf-8"}));
      const anchor = document.createElement("a");
      anchor.href = blobURL;
      anchor.download = data.run_id + (complete ? "-labels.jsonl" : "-label-draft.jsonl");
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      setTimeout(() => URL.revokeObjectURL(blobURL), 0);
      elements.status.textContent = complete ? "Completed labels exported." : "Draft exported.";
    }

    function validateImportedDocument(document, row, index) {
      if (!document || document.schema_version !== 1 || document.run_id !== data.run_id || document.case_id !== row.case_id || document.case_sha256 !== row.case_sha256 || document.url !== row.url) {
        throw new Error("row " + (index + 1) + " does not match this run");
      }
      for (const field of ["intent", "query", "title", "domain", "rank"]) {
        if (Object.prototype.hasOwnProperty.call(document, field) && document[field] !== row[field]) {
          throw new Error("row " + (index + 1) + " altered " + field);
        }
      }
      if (!validGrade(document.relevance)) {
        throw new Error("row " + (index + 1) + " has an invalid grade");
      }
      return document.relevance;
    }

    async function importDraft(file) {
      try {
        const documents = (await file.text()).split(/\r?\n/).filter(line => line.trim() !== "").map(line => JSON.parse(line));
        if (documents.length !== rows.length) throw new Error("draft contains " + documents.length + " rows; expected " + rows.length);
        const imported = documents.map((document, index) => validateImportedDocument(document, rows[index], index));
        imported.forEach((grade, index) => grades[index] = grade);
        current = Math.max(0, nextUngraded(rows.length - 1));
        persist();
        elements.status.textContent = "Draft imported: " + gradedCount() + " of " + rows.length + " graded.";
        render();
      } catch (error) {
        elements.status.textContent = "Import failed: " + error.message;
      }
    }

    document.querySelectorAll("[data-grade]").forEach(button => button.addEventListener("click", () => setGrade(Number(button.dataset.grade))));
    document.getElementById("previous").addEventListener("click", () => { if (current > 0) current--; render(); });
    document.getElementById("next").addEventListener("click", () => { if (current < rows.length - 1) current++; render(); });
    document.getElementById("next-ungraded").addEventListener("click", () => {
      const next = nextUngraded(current);
      if (next >= 0) current = next;
      else elements.status.textContent = "All judgments are complete.";
      render();
    });
    document.getElementById("export-draft").addEventListener("click", () => downloadLabels(false));
    document.getElementById("export-complete").addEventListener("click", () => downloadLabels(true));
    document.getElementById("import-draft").addEventListener("change", event => {
      const file = event.target.files[0];
      if (file) importDraft(file);
      event.target.value = "";
    });
    document.addEventListener("keydown", event => {
      if (event.metaKey || event.ctrlKey || event.altKey || event.target.tagName === "INPUT") return;
      if (/^[0-3]$/.test(event.key)) { event.preventDefault(); setGrade(Number(event.key)); }
      else if (event.key === "ArrowLeft" && current > 0) { event.preventDefault(); current--; render(); }
      else if (event.key === "ArrowRight" && current < rows.length - 1) { event.preventDefault(); current++; render(); }
      else if (event.key.toLowerCase() === "u") { event.preventDefault(); const next = nextUngraded(current); if (next >= 0) current = next; render(); }
    });

    restore();
    createRail();
    render();
  </script>
</body>
</html>
`))
