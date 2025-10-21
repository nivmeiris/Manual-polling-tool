// static/js/main.js
document.addEventListener('DOMContentLoaded', () => {

    // --- Data Definitions for Checkboxes ---
    const definitions = {
        admob: {
            dimensions: ["DATE", "AD_UNIT", "FORMAT", "COUNTRY", "APP", "PLATFORM", "PLACEMENT", "AD_SOURCE", "CREATIVE", "BID_TYPE"],
            metrics: ["ESTIMATED_EARNINGS", "IMPRESSIONS", "IMPRESSION_RPM", "CLICKS", "ACTIVE_VIEW_IMPRESSIONS", "MATCHED_REQUESTS", "SHOW_RATE", "CLICK_THROUGH_RATE", "AD_REQUESTS"]
        },
        gam: {
            dimensions: ["date", "ad_unit_name", "ad_unit_id", "parent_ad_unit_id", "country_name", "country_criteria_id"],
            metrics: ["impressions", "revenue"]
        },
        applovin: {
            dimensions: ["day", "application", "country", "platform", "package_name", "size", "zone_id", "ad_type"],
            metrics: ["revenue", "views", "impressions", "clicks"]
        },
        chartboost: {
            dimensions: ["day", "app_id", "app_name", "country_code", "platform", "ad_location", "ad_type", "campaign_type"],
            metrics: ["revenue", "impressions", "clicks", "installs", "ecpm", "cpcv", "ctr", "install_rate", "video_completed"]
        },
        facebook: {
            dimensions: ["placement", "country", "day"],
            metrics: ["revenue"]
        },
        fyber: {
            dimensions: ["app_id", "spot_id", "day", "country_code", "content_id", "content_name", "app_name", "distributor_name", "content_categories"],
            metrics: ["ad_requests", "impressions", "fill_rate", "clicks", "ctr", "ecpm", "revenue"]
        },
        hyprmx: {
            dimensions: ["placement", "country"], // מבוסס על הדוגמה שלך
            metrics: [] // ה-API שסיפקת לא משתמש בזה, אז נשאיר ריק
        },
        inmobi: {
            dimensions: ["country", "countryId", "placementId", "placementName"],
            metrics: ["earnings"]
        }
    };

    // --- Function to Create Checkboxes ---
    function createCheckboxes(containerId, items) {
        const container = document.getElementById(containerId);
        if (!container) return;
        const type = containerId.endsWith('-dimensions') ? 'dimensions' : 'metrics';
        container.innerHTML = `<strong>${type.charAt(0).toUpperCase() + type.slice(1)}:</strong>`;
        items.forEach(item => {
            const label = document.createElement('label');
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.value = item;
            checkbox.checked = true; // Default to checked
            label.appendChild(checkbox);
            label.appendChild(document.createTextNode(` ${item.replace(/_/g, ' ')}`));
            container.appendChild(label);
        });
    }

    // --- Populate all checkboxes ---
    for (const [provider, cats] of Object.entries(definitions)) {
        createCheckboxes(`${provider}-dimensions`, cats.dimensions);
        createCheckboxes(`${provider}-metrics`, cats.metrics);
    }

    // --- Set default dates ---
    function setDefaultDates() {
        const today = new Date();
        const sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(today.getDate() - 7);

        const formatDate = (date) => date.toISOString().split('T')[0];

        document.querySelectorAll('input[type="date"]').forEach(input => {
            if (input.id.includes('-end-date')) {
                input.value = formatDate(today);
            } else if (input.id.includes('-start-date')) {
                input.value = formatDate(sevenDaysAgo);
            }
        });
    }
    setDefaultDates();

    // --- Tab Switching Logic ---
    const tabButtons = document.querySelectorAll('.tab-button');
    const tabPanes = document.querySelectorAll('.tab-pane');

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            tabButtons.forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');

            tabPanes.forEach(pane => {
                if (pane.id === button.dataset.tab) {
                    pane.classList.add('active');
                } else {
                    pane.classList.remove('active');
                }
            });
        });
    });

    // --- Form Submission Logic ---
    document.querySelectorAll('form').forEach(form => {
        form.addEventListener('submit', async (event) => {
            event.preventDefault();

            const provider = form.id.replace('-form', '');
            const statusEl = document.getElementById(`${provider}-status`);
            const resultsEl = document.getElementById(`${provider}-results`);
            const endpoint = form.dataset.endpoint;

            statusEl.className = 'status loading';
            statusEl.textContent = 'Polling in progress... Please wait.';
            resultsEl.innerHTML = '';

            const payload = {};

            // Collect data from inputs and selects
            form.querySelectorAll('input, select').forEach(input => {
                if (input.type !== 'checkbox') {
                    const key = input.id.replace(`${provider}-`, '').replace(/-/g, '_');
                    payload[key] = input.value;
                }
            });

            // Collect data from checkboxes
            payload.dimensions = Array.from(form.querySelectorAll(`#${provider}-dimensions input:checked`)).map(cb => cb.value);
            payload.metrics = Array.from(form.querySelectorAll(`#${provider}-metrics input:checked`)).map(cb => cb.value);

            try {
                const response = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                const result = await response.json();

                if (!response.ok) {
                    throw new Error(result.error || `Server responded with status ${response.status}`);
                }

                statusEl.className = 'status success';
                const data = result.data || result;
                const rowCount = Array.isArray(data) ? data.length : (data.data ? data.data.length : 0);
                statusEl.textContent = `Polling successful! Fetched ${rowCount} rows.`;

                // Display results and download link
                displayResults(resultsEl, result, `${provider}_report.json`);

            } catch (error) {
                statusEl.className = 'status error';
                statusEl.textContent = `Polling failed: ${error.message}`;
                console.error(error);
            }
        });
    });

    function displayResults(container, resultData, filename) {
        container.innerHTML = ''; // Clear previous results

        // Create Download Link
        const blob = new Blob([JSON.stringify(resultData, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.textContent = 'Download Results (JSON)';
        container.appendChild(a);

        // Display JSON in <pre> tag
        const pre = document.createElement('pre');
        pre.textContent = JSON.stringify(resultData, null, 2);
        container.appendChild(pre);
    }
});