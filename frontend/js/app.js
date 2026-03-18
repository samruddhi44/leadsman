const pages = document.querySelectorAll(".page");
const navItems = document.querySelectorAll(".nav-item");
const chips = document.querySelectorAll(".chip");

let activePlatforms = ["facebook", "instagram", "linkedin", "youtube"];
let poller = null;

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
    activePlatforms = Array.from(document.querySelectorAll(".chip.active")).map((c) => c.dataset.platform);
  });
});

function showLoader(mode) {
  const id = mode === "google_business" ? "gb-loader" : "sl-loader";
  const el = document.getElementById(id);
  if (el) el.style.display = "flex";
}

function hideLoader(mode) {
  const id = mode === "google_business" ? "gb-loader" : "sl-loader";
  const el = document.getElementById(id);
  if (el) el.style.display = "none";
}

function startPolling(mode) {
  if (poller) clearInterval(poller);

  poller = setInterval(async () => {
    try {
      const res = await fetch(`/api/progress?mode=${mode}&t=${Date.now()}`);
      const data = await res.json();

      updateUI(data, mode);

      if (!data.running) {
        clearInterval(poller);
        poller = null;
        hideLoader(mode);
      }
    } catch (error) {
      console.error("Polling error:", error);
      hideLoader(mode);
    }
  }, 1000);
}

function updateUI(data, mode) {
  const prefix = mode === "google_business" ? "gb" : "sl";

  document.getElementById(`${prefix}-progress-label`).textContent =
    `Progress (${data.current}/${data.total})`;

  document.getElementById(`${prefix}-results-label`).textContent =
    `Results (${data.results.length})`;

  const percent = data.total > 0 ? (data.current / data.total) * 100 : 0;
  document.getElementById(`${prefix}-progress-fill`).style.width = `${percent}%`;

  if (mode === "google_business") renderGoogleTable(data.results);

  if (mode === "social_lookup") {
    renderSocialTable(data.results);
    const logBox = document.getElementById("sl-log-box");
    logBox.innerHTML = (data.logs || []).slice(-10).join("<br>");
  }
}

function renderGoogleTable(rows) {
  const body = document.getElementById("gb-table-body");

  if (!rows || rows.length === 0) {
    body.innerHTML = `<tr><td colspan="19" class="empty-state">No results yet.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((r) => `
    <tr>
      <td>${r.company_name || ""}</td>
      <td>${r.keyword || ""}</td>
      <td>${r.location || ""}</td>
      <td>${r.category || ""}</td>
      <td>${r.address || ""}</td>
      <td>${renderLink(r.website)}</td>
      <td>${r.phone_number || ""}</td>
      <td>${r.email_1 || ""}</td>
      <td>${r.email_2 || ""}</td>
      <td>${r.rating || ""}</td>
      <td>${r.reviews_count || ""}</td>
      <td>${renderLink(r.map_link, "Map")}</td>
      <td>${r.cid || ""}</td>
      <td>${r.opening_hours || ""}</td>
      <td>${renderImage(r.featured_image)}</td>
      <td>${r.city || ""}</td>
      <td>${r.state || ""}</td>
      <td>${r.pincode || ""}</td>
      <td>${r.country_code || ""}</td>
    </tr>
  `).join("");
}

function renderSocialTable(rows) {
  const body = document.getElementById("sl-table-body");

  if (!rows || rows.length === 0) {
    body.innerHTML = `<tr><td colspan="8" class="empty-state">No results yet.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((r) => `
    <tr>
      <td>${r.title || ""}</td>
      <td>${r.domain || ""}</td>
      <td>${r.phones || ""}</td>
      <td>${r.emails || ""}</td>
      <td>${renderLink(r.link)}</td>
      <td>${r.source || ""}</td>
      <td>${r.category || ""}</td>
      <td>${r.location || ""}</td>
    </tr>
  `).join("");
}

function renderLink(url, label = "Open") {
  if (!url) return "";
  return `<a href="${url}" target="_blank">${label}</a>`;
}

function renderImage(url) {
  if (!url) return "";
  return `<img class="thumb" src="${url}" alt="img" onerror="this.style.display='none';" />`;
}

async function postJSON(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return await res.json();
}

document.getElementById("gb-search").addEventListener("click", async () => {
  const keywords = document.getElementById("gb-keywords").value;
  const locations = document.getElementById("gb-locations").value;
  const enable_email_scraping = document.getElementById("gb-email-scrape").checked;

  showLoader("google_business");

  document.getElementById("gb-table-body").innerHTML =
    `<tr><td colspan="19" class="empty-state">Scraping started...</td></tr>`;

  await postJSON("/api/google-business/start", {
    keywords,
    locations,
    enable_email_scraping,
  });

  startPolling("google_business");
});

document.getElementById("sl-search").addEventListener("click", async () => {
  const keyword = document.getElementById("sl-keyword").value;
  const max_pages = Number(document.getElementById("sl-max-pages").value);

  showLoader("social_lookup");

  document.getElementById("sl-table-body").innerHTML =
    `<tr><td colspan="8" class="empty-state">Scraping started...</td></tr>`;

  await postJSON("/api/social-lookup/start", {
    keyword,
    max_pages,
    platforms: activePlatforms,
  });

  startPolling("social_lookup");
});

document.getElementById("gb-stop").addEventListener("click", async () => {
  await postJSON("/api/stop", { mode: "google_business" });
  hideLoader("google_business");
});

document.getElementById("sl-stop").addEventListener("click", async () => {
  await postJSON("/api/stop", { mode: "social_lookup" });
  hideLoader("social_lookup");
});

document.getElementById("gb-export-csv").addEventListener("click", () => {
  window.open("/api/export?mode=google_business&format=csv", "_blank");
});

document.getElementById("gb-export-xlsx").addEventListener("click", () => {
  window.open("/api/export?mode=google_business&format=xlsx", "_blank");
});

document.getElementById("sl-export-csv").addEventListener("click", () => {
  window.open("/api/export?mode=social_lookup&format=csv", "_blank");
});

document.getElementById("sl-export-xlsx").addEventListener("click", () => {
  window.open("/api/export?mode=social_lookup&format=xlsx", "_blank");
});