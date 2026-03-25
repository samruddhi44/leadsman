const pages = document.querySelectorAll(".page");
const navItems = document.querySelectorAll(".nav-item");
const chips = document.querySelectorAll(".chip");
const dropdowns = document.querySelectorAll(".dropdown-wrap");

let activePlatforms = ["facebook", "instagram", "linkedin", "youtube"];
let activePollers = {
  google_business: null,
  social_lookup: null,
};
let lastLogCounts = {
  google_business: 0,
  social_lookup: 0,
};

navItems.forEach((item) => {
  item.addEventListener("click", () => {
    navItems.forEach((btn) => btn.classList.remove("active"));
    item.classList.add("active");

    const pageId = item.dataset.page;
    pages.forEach((page) => page.classList.remove("active-page"));
    document.getElementById(pageId).classList.add("active-page");
  });
});

chips.forEach((chip) => {
  chip.addEventListener("click", () => {
    chip.classList.toggle("active");
    activePlatforms = Array.from(document.querySelectorAll(".chip.active")).map(
      (c) => c.dataset.platform
    );
  });
});

function closeDropdown(dropdown) {
  if (!dropdown) return;
  dropdown.classList.remove("open");
  const button = dropdown.querySelector(".dropdown-btn");
  if (button) button.setAttribute("aria-expanded", "false");
}

function closeAllDropdowns(exceptDropdown = null) {
  dropdowns.forEach((dropdown) => {
    if (dropdown !== exceptDropdown) {
      closeDropdown(dropdown);
    }
  });
}

dropdowns.forEach((dropdown) => {
  const button = dropdown.querySelector(".dropdown-btn");
  const menuButtons = dropdown.querySelectorAll(".dropdown-menu button");

  if (!button) return;

  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();

    const willOpen = !dropdown.classList.contains("open");
    closeAllDropdowns(dropdown);
    dropdown.classList.toggle("open", willOpen);
    button.setAttribute("aria-expanded", String(willOpen));
  });

  menuButtons.forEach((menuButton) => {
    menuButton.addEventListener("click", () => {
      closeDropdown(dropdown);
    });
  });
});

document.addEventListener("click", (event) => {
  if (!event.target.closest(".dropdown-wrap")) {
    closeAllDropdowns();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeAllDropdowns();
  }
});

// --- Log panel toggle ---
document.querySelectorAll(".log-toggle").forEach((btn) => {
  btn.addEventListener("click", () => {
    const content = btn.nextElementSibling;
    const isExpanded = content.classList.toggle("expanded");
    btn.textContent = isExpanded ? "▼ Live Logs" : "▶ Live Logs";
  });
});

function escapeHtml(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function normalizeHttpUrl(url) {
  if (!url) return "";

  try {
    const parsed = new URL(String(url), window.location.origin);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return "";
    }
    return parsed.href;
  } catch {
    return "";
  }
}

function renderLink(url, label = "Open") {
  const safeUrl = normalizeHttpUrl(url);
  if (!safeUrl) return "";
  return `<a href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
}

function renderImage(url) {
  const safeUrl = normalizeHttpUrl(url);
  if (!safeUrl) return "";
  return `<img class="thumb" src="${escapeHtml(safeUrl)}" alt="featured image" loading="lazy" onerror="this.closest('.thumb-link')?.classList.add('thumb-link-broken'); this.style.display='none';" />`;
}

function renderValue(value, fallback = "Not found") {
  const text = String(value ?? "").trim();
  return escapeHtml(text || fallback);
}

function firstFilledValue(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function renderEmailValue(row) {
  const primary = firstFilledValue(row.email);
  if (primary) return renderValue(primary);

  const legacyEmails = [row.email_1, row.email_2]
    .map((value) => String(value ?? "").trim())
    .filter(Boolean);

  if (legacyEmails.length) {
    return escapeHtml(Array.from(new Set(legacyEmails)).join(", "));
  }

  return "Not found";
}

function renderImageCell(url, label = "Open image") {
  const safeUrl = normalizeHttpUrl(url);
  if (!safeUrl) return "Not found";

  const image = renderImage(safeUrl);
  if (!image) return "Not found";

  return `
    <a class="thumb-link" href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">
      ${image}
      <span class="thumb-caption">${escapeHtml(label)}</span>
    </a>
  `.trim();
}

function describeGooglePageDepth(maxPages) {
  return maxPages === 0
    ? "all available Google pages"
    : `up to ${maxPages} Google page${maxPages === 1 ? "" : "s"}`;
}

function setStatus(mode, text) {
  const el = document.getElementById(
    mode === "google_business" ? "gb-status-text" : "sl-status-text"
  );
  if (el) el.textContent = text;
}

function showLoader(mode, text = "Scraping in progress...") {
  const el = document.getElementById(
    mode === "google_business" ? "gb-loader-box" : "sl-loader-box"
  );
  if (el) el.style.display = "flex";
  setStatus(mode, text);
}

function hideLoader(mode) {
  const el = document.getElementById(
    mode === "google_business" ? "gb-loader-box" : "sl-loader-box"
  );
  if (el) el.style.display = "none";
}

function setButtonsDisabled(mode, running) {
  const map = {
    google_business: {
      search: document.getElementById("gb-search"),
      stop: document.getElementById("gb-stop"),
    },
    social_lookup: {
      search: document.getElementById("sl-search"),
      stop: document.getElementById("sl-stop"),
    },
  };

  const group = map[mode];
  if (!group) return;

  group.search.disabled = running;
  group.stop.disabled = !running;
}

function stopPolling(mode) {
  if (activePollers[mode]) {
    clearInterval(activePollers[mode]);
    activePollers[mode] = null;
  }
}

function renderLogs(logs, mode) {
  const prefix = mode === "google_business" ? "gb" : "sl";
  const container = document.getElementById(`${prefix}-log-entries`);
  if (!container) return;

  const prevCount = lastLogCounts[mode] || 0;
  if (logs.length === prevCount) return;

  lastLogCounts[mode] = logs.length;

  const html = logs
    .map(
      (log, i) =>
        `<div class="log-line" style="animation-delay: ${Math.max(0, (i - prevCount) * 0.04)}s">› ${escapeHtml(log)}</div>`
    )
    .join("");

  container.innerHTML = html;

  // Auto-scroll to bottom
  const logContent = document.getElementById(`${prefix}-log-content`);
  if (logContent) {
    logContent.scrollTop = logContent.scrollHeight;
  }
}

function startPolling(mode) {
  stopPolling(mode);

  activePollers[mode] = setInterval(async () => {
    try {
      const res = await fetch(`/api/progress?mode=${mode}&t=${Date.now()}`, {
        cache: "no-store",
      });

      if (!res.ok) throw new Error(`Progress request failed: ${res.status}`);

      const data = await res.json();
      updateUI(data, mode);

      if (!data.running) {
        stopPolling(mode);
        hideLoader(mode);
        setButtonsDisabled(mode, false);

        setStatus(
          mode,
          data.stop
            ? "Scraping stopped. Existing data is kept in the table."
            : "Scraping finished successfully."
        );
      }
    } catch (error) {
      console.error("Polling error:", error);
      stopPolling(mode);
      hideLoader(mode);
      setButtonsDisabled(mode, false);
      setStatus(mode, "Connection issue while checking progress.");
    }
  }, 400);
}

function updateUI(data, mode) {
  const prefix = mode === "google_business" ? "gb" : "sl";

  const progressLabel = document.getElementById(`${prefix}-progress-label`);
  const resultsLabel = document.getElementById(`${prefix}-results-label`);
  const progressFill = document.getElementById(`${prefix}-progress-fill`);

  const current = Number(data.current || 0);
  const total = Number(data.total || 0);
  const results = Array.isArray(data.results) ? data.results : [];
  const logs = Array.isArray(data.logs) ? data.logs : [];

  if (progressLabel) progressLabel.textContent = `Progress (${current}/${total})`;
  if (resultsLabel) resultsLabel.textContent = `Results (${results.length})`;

  const percent = total > 0 ? Math.min((current / total) * 100, 100) : 0;
  if (progressFill) progressFill.style.width = `${percent}%`;

  if (mode === "google_business") renderGoogleTable(results);
  if (mode === "social_lookup") renderSocialTable(results);

  renderLogs(logs, mode);
}

function renderGoogleTable(rows) {
  const body = document.getElementById("gb-table-body");
  if (!body) return;

  if (!rows || rows.length === 0) {
    if (body.children.length !== 1 || !body.children[0].querySelector(".empty-state")) {
      body.innerHTML = `<tr><td colspan="7" class="empty-state">No results yet.</td></tr>`;
    }
    return;
  }

  // Remove empty state if present
  if (body.children.length === 1 && body.children[0].querySelector(".empty-state")) {
    body.innerHTML = "";
  }

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const businessName = firstFilledValue(r.business_name, r.company_name, r.title);
    const city = firstFilledValue(r.city, r.location);
    const mapLink = firstFilledValue(r.map_link, r.link);
    const fullAddress = firstFilledValue(r.full_address, r.address);
    const featuredImageUrl = firstFilledValue(r.featured_image_url, r.featured_image);
    const pinCode = firstFilledValue(r.pin_code, r.pincode);
    const html = `
      <td>${renderValue(businessName)}</td>
      <td>${renderValue(city)}</td>
      <td>${renderLink(mapLink, "Map") || "Not found"}</td>
      <td>${renderValue(fullAddress)}</td>
      <td>${renderEmailValue(r)}</td>
      <td>${renderImageCell(featuredImageUrl, "Open image")}</td>
      <td>${renderValue(pinCode)}</td>
    `.trim();

    if (i < body.children.length) {
      if (body.children[i].innerHTML.trim() !== html) {
        body.children[i].innerHTML = html;
      }
    } else {
      const tr = document.createElement("tr");
      tr.innerHTML = html;
      body.appendChild(tr);
    }
  }

  while (body.children.length > rows.length) {
    body.removeChild(body.lastElementChild);
  }
}

function renderSocialTable(rows) {
  const body = document.getElementById("sl-table-body");
  if (!body) return;

  if (!rows || rows.length === 0) {
    if (body.children.length !== 1 || !body.children[0].querySelector(".empty-state")) {
      body.innerHTML = `<tr><td colspan="6" class="empty-state">No results yet.</td></tr>`;
    }
    return;
  }

  // Remove empty state if present
  if (body.children.length === 1 && body.children[0].querySelector(".empty-state")) {
    body.innerHTML = "";
  }

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const profileName = firstFilledValue(r.profile_name, r.title);
    const platform = firstFilledValue(r.platform, r.source);
    const profileLink = firstFilledValue(r.profile_link, r.link);
    const bio = firstFilledValue(r.bio, r.description);
    const followers = firstFilledValue(r.followers);
    const contactInfo = firstFilledValue(r.contact_info, r.emails, r.phones);
    const html = `
      <td>${renderValue(profileName)}</td>
      <td>${renderValue(platform)}</td>
      <td>${renderLink(profileLink, "Open") || "Not found"}</td>
      <td>${renderValue(bio)}</td>
      <td>${renderValue(followers)}</td>
      <td>${renderValue(contactInfo)}</td>
    `.trim();

    if (i < body.children.length) {
      if (body.children[i].innerHTML.trim() !== html) {
        body.children[i].innerHTML = html;
      }
    } else {
      const tr = document.createElement("tr");
      tr.innerHTML = html;
      body.appendChild(tr);
    }
  }

  while (body.children.length > rows.length) {
    body.removeChild(body.lastElementChild);
  }
}

async function readErrorMessage(res) {
  const contentType = res.headers.get("content-type") || "";

  if (contentType.includes("application/json")) {
    try {
      const data = await res.json();
      if (data && typeof data.detail === "string") {
        return data.detail;
      }
      if (data && typeof data.message === "string") {
        return data.message;
      }
    } catch {
      return `Request failed: ${res.status}`;
    }
  }

  const text = await res.text();
  return text || `Request failed: ${res.status}`;
}

async function postJSON(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    throw new Error(await readErrorMessage(res));
  }

  return await res.json();
}

function triggerDownload(mode, format) {
  const link = document.createElement("a");
  link.href = `/api/export?mode=${encodeURIComponent(mode)}&format=${encodeURIComponent(format)}`;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function resetLogs(mode) {
  lastLogCounts[mode] = 0;
  const prefix = mode === "google_business" ? "gb" : "sl";
  const container = document.getElementById(`${prefix}-log-entries`);
  if (container) container.innerHTML = "";

  // Auto-expand logs when scraping starts
  const logContent = document.getElementById(`${prefix}-log-content`);
  const logToggle = document.getElementById(`${prefix}-log-toggle`);
  if (logContent && !logContent.classList.contains("expanded")) {
    logContent.classList.add("expanded");
    if (logToggle) logToggle.textContent = "▼ Live Logs";
  }
}

document.getElementById("gb-search").addEventListener("click", async () => {
  const keywords = document.getElementById("gb-keywords").value.trim();
  const locations = document.getElementById("gb-locations").value.trim();
  const enable_email_scraping = document.getElementById("gb-email-scrape").checked;
  const requestedMaxPages = Number(document.getElementById("gb-max-pages").value);
  const max_pages = Math.max(
    0,
    Math.min(Number.isFinite(requestedMaxPages) ? requestedMaxPages : 0, 10)
  );
  const pageDepthText = describeGooglePageDepth(max_pages);

  if (!keywords || !locations) {
    setStatus("google_business", "Please enter both keywords and locations.");
    return;
  }

  try {
    setButtonsDisabled("google_business", true);
    resetLogs("google_business");
    showLoader(
      "google_business",
      enable_email_scraping
        ? `Searching Google Business data across ${pageDepthText}. Core leads appear first, then emails are enriched...`
        : `Searching Google Business data across ${pageDepthText}...`
    );
    setStatus("google_business", "Scraping started...");

    document.getElementById("gb-table-body").innerHTML =
      `<tr><td colspan="7" class="empty-state">Scraping started. Clean Google Business leads will appear here live...</td></tr>`;

    await postJSON("/api/google-business/start", {
      keywords,
      locations,
      enable_email_scraping,
      max_pages,
    });

    startPolling("google_business");
  } catch (error) {
    console.error(error);
    hideLoader("google_business");
    setButtonsDisabled("google_business", false);
    setStatus("google_business", error.message || "Failed to start scraping.");
  }
});

document.getElementById("sl-search").addEventListener("click", async () => {
  const keyword = document.getElementById("sl-keyword").value.trim();
  const location = document.getElementById("sl-location").value.trim();
  const max_pages = Number(document.getElementById("sl-max-pages").value);

  if (!keyword) {
    setStatus("social_lookup", "Please enter a keyword.");
    return;
  }

  if (!activePlatforms.length) {
    setStatus("social_lookup", "Please select at least one platform.");
    return;
  }

  try {
    setButtonsDisabled("social_lookup", true);
    resetLogs("social_lookup");
    showLoader("social_lookup", "Searching selected social platforms directly with Playwright...");
    setStatus("social_lookup", "Social lookup started...");

    document.getElementById("sl-table-body").innerHTML =
      `<tr><td colspan="6" class="empty-state">Scraping started. Relevant social profiles will appear here live...</td></tr>`;

    await postJSON("/api/social-lookup/start", {
      keyword,
      location,
      max_pages,
      platforms: activePlatforms,
    });

    startPolling("social_lookup");
  } catch (error) {
    console.error(error);
    hideLoader("social_lookup");
    setButtonsDisabled("social_lookup", false);
    setStatus("social_lookup", error.message || "Failed to start social lookup.");
  }
});

document.getElementById("gb-stop").addEventListener("click", async () => {
  try {
    setStatus("google_business", "Stopping scraping...");
    await postJSON("/api/stop", { mode: "google_business" });
  } catch (error) {
    console.error(error);
    setStatus("google_business", error.message || "Stop request failed.");
  }
});

document.getElementById("sl-stop").addEventListener("click", async () => {
  try {
    setStatus("social_lookup", "Stopping scraping...");
    await postJSON("/api/stop", { mode: "social_lookup" });
  } catch (error) {
    console.error(error);
    setStatus("social_lookup", error.message || "Stop request failed.");
  }
});

document.getElementById("gb-export-csv").addEventListener("click", () => {
  triggerDownload("google_business", "csv");
});

document.getElementById("gb-export-xlsx").addEventListener("click", () => {
  triggerDownload("google_business", "xlsx");
});

document.getElementById("sl-export-csv").addEventListener("click", () => {
  triggerDownload("social_lookup", "csv");
});

document.getElementById("sl-export-xlsx").addEventListener("click", () => {
  triggerDownload("social_lookup", "xlsx");
});

// Initialize button states — stop disabled by default
setButtonsDisabled("google_business", false);
setButtonsDisabled("social_lookup", false);
