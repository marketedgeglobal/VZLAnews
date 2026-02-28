(function () {
    async function loadJson(path) {
        const response = await fetch(path, { cache: 'no-store' });
        if (!response.ok) throw new Error('Failed to load ' + path);
        return response.json();
    }

    const iconMap = {
        RISK: '⚠️',
        OPPORTUNITY: '✅',
        POLICY: '🏛️',
        ENERGY: '⛽',
        FX: '💱',
        TRADE: '🚢',
        HUMAN: '🧭',
        NEW: '🆕'
    };

    function esc(value) {
        return String(value || '').replace(/[&<>"']/g, (char) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char] || char
        ));
    }

    function list(items) {
        return `<ul>${items.map((item) => `<li>${esc(item)}</li>`).join('')}</ul>`;
    }

    function renderDevelopment(item, idx) {
        if (typeof item === 'string') {
            return `<li>${esc(item)}</li>`;
        }
        if (!item || typeof item !== 'object') {
            return '<li>Monitoring continues for concrete policy and operational developments.</li>';
        }
        const links = (item.itemIds || []).slice(0, 3)
            .map((id, sourceIdx) => `<a href="#item-${esc(id)}">[${sourceIdx + 1}]</a>`)
            .join(' ');
        const sourceLine = links ? ` <span class="sources">Sources: ${links}</span>` : '';
        return `<li>${esc(item.text || ('Development ' + (idx + 1)))}${sourceLine}</li>`;
    }

    function renderNumbers(items) {
        return `<ul>${(items || []).slice(0, 3).map((n) => {
            const label = esc(n && n.label ? n.label : 'Quantitative signal');
            const value = esc(n && n.value ? n.value : 'N/A');
            const context = n && n.context ? ` (${esc(n.context)})` : '';
            const source = n && n.itemId ? ` <a href="#item-${esc(n.itemId)}">[source]</a>` : '';
            return `<li><strong>${label}:</strong> ${value}${context}${source}</li>`;
        }).join('')}</ul>`;
    }

    function renderTop(highlights) {
        return `
            <section class="top-row">
                <article class="panel">
                    <h3>Key Developments</h3>
                    <ul>${(highlights.keyDevelopments || []).slice(0, 5).map(renderDevelopment).join('')}</ul>
                </article>
                <article class="panel">
                    <h3>By the Numbers</h3>
                    ${renderNumbers(highlights.byTheNumbers)}
                </article>
            </section>
        `;
    }

    async function loadIMF() {
        try {
            return await loadJson('data/imf_ven.json');
        } catch {
            return null;
        }
    }

    function fmt(value, unit) {
        if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
        const numeric = Number(value);
        const absValue = Math.abs(numeric);
        const decimals = absValue >= 100 ? 0 : absValue >= 10 ? 1 : 2;
        const rendered = numeric.toFixed(decimals);
        const normalizedUnit = String(unit || '').toLowerCase();
        return normalizedUnit.includes('percent') || normalizedUnit.includes('%') ? `${rendered}%` : rendered;
    }

    function spark(series) {
        if (!Array.isArray(series) || series.length < 2) return '';
        const last = series.slice(-10);
        const values = last.map((d) => Number(d.value)).filter((v) => Number.isFinite(v));
        if (values.length < 2) return '';
        const min = Math.min(...values);
        const max = Math.max(...values);
        const w = 90;
        const h = 22;
        const p = 2;
        const x = (index) => p + (index * (w - (2 * p)) / (last.length - 1));
        const y = (value) => (max === min ? h / 2 : p + ((h - (2 * p)) * (1 - ((value - min) / (max - min)))));
        const points = last.map((point, idx) => `${x(idx)},${y(Number(point.value))}`).join(' ');
        return `<svg class="spark" viewBox="0 0 ${w} ${h}" aria-hidden="true"><polyline fill="none" points="${points}" /></svg>`;
    }

    function renderIMFCard(data) {
        if (!data || !Array.isArray(data.metrics) || !data.metrics.length) return '';
        const tiles = data.metrics.map((metric) => {
            const year = metric && metric.latest && metric.latest.year ? metric.latest.year : '';
            const value = fmt(metric && metric.latest ? metric.latest.value : null, metric && metric.unit ? metric.unit : '');
            const deltaValue = metric ? metric.delta : null;
            const delta = deltaValue === null || deltaValue === undefined || Number.isNaN(Number(deltaValue))
                ? '—'
                : (Number(deltaValue) >= 0 ? `+${fmt(deltaValue, metric.unit || '')}` : fmt(deltaValue, metric.unit || ''));
            return `
                <div class="metric-tile">
                    <div class="metric-top">
                        <div class="metric-label">${esc(metric.label || metric.code || 'Indicator')}</div>
                        <div class="metric-year">${esc(year)}</div>
                    </div>
                    <div class="metric-value">${esc(value)}</div>
                    <div class="metric-bottom">
                        <div class="metric-delta">YoY: ${esc(delta)}</div>
                        ${spark(metric.series || [])}
                    </div>
                </div>
            `;
        }).join('');

        const asOf = typeof data.asOf === 'string' && data.asOf.length >= 10 ? data.asOf.slice(0, 10) : '—';
        return `
            <section class="panel imf-card">
                <div class="card-head">
                    <div>
                        <h3>IMF Macro Snapshot</h3>
                        <div class="meta">Updated: ${esc(asOf)}</div>
                    </div>
                    <a class="small-link" href="https://www.imf.org/external/datamapper/profile/VEN" target="_blank" rel="noopener">Open IMF Profile</a>
                </div>
                <div class="metric-grid">${tiles}</div>
            </section>
        `;
    }

    function renderItem(item) {
        const icons = (item.icons || []).map((icon) => `<span class="item-icon" title="${esc(icon)}">${iconMap[icon] || '•'}</span>`).join('');
        const evidence = (item.insight2 && item.insight2.evidence ? item.insight2.evidence : []).slice(0, 2);
        const evidenceHtml = evidence.length
            ? `<div class="item-evidence">${evidence.map((line) => `<p>${esc(line)}</p>`).join('')}</div>`
            : '';
        const preview = (item.preview || '').trim();
        const description = preview.length >= 80 ? preview : 'Open the source for details.';

        return `
            <article class="item-card">
                <div class="item-head">
                    <h5><a id="item-${esc(item.id)}"></a><a href="${esc(item.url)}" target="_blank" rel="noopener">${esc(item.title)}</a></h5>
                    <div class="item-icons">${icons}</div>
                </div>
                <p class="item-meta">${esc(item.publisher)} · ${esc(item.publishedAt)} · ${esc(item.sourceTier)}</p>
                <p class="item-desc">${esc(description)}</p>
                ${evidenceHtml}
            </article>
        `;
    }

    function renderSectors(latest) {
        return (latest.sectors || []).map((sector) => {
            const bullets = sector.synth && sector.synth.bullets ? sector.synth.bullets.slice(0, 3) : [];
            return `
                <section class="sector-block">
                    <h3>${esc(sector.name)}</h3>
                    <div class="sector-synth">${list(bullets)}</div>
                    <div class="items-grid">${(sector.items || []).map(renderItem).join('')}</div>
                </section>
            `;
        }).join('');
    }

    function renderMacros(macros) {
        return `
            <section class="macro-block">
                <h3>Macro Indicators</h3>
                <p class="macro-note">Daily refresh at end-of-report for context and trend checks.</p>
                <div class="macro-grid">
                    ${(macros.indicators || []).map((m) => `
                        <article class="macro-card">
                            <h4>${esc(m.name)}</h4>
                            <p class="macro-value">${esc(m.value)}</p>
                            <p class="macro-trend">${esc(m.trend)}</p>
                        </article>
                    `).join('')}
                </div>
            </section>
        `;
    }

    async function init() {
        const root = document.getElementById('app-root');
        if (!root) return;
        try {
            const [latest, highlights, macros, imf] = await Promise.all([
                loadJson('data/latest.json'),
                loadJson('data/highlights.json'),
                loadJson('data/macros.json'),
                loadIMF()
            ]);

            root.innerHTML = `
                <section class="exec-brief panel">
                    <h2>Executive Brief</h2>
                    ${list((highlights.executiveBriefBullets || []).slice(0, 5))}
                </section>
                ${renderTop(highlights)}
                ${renderIMFCard(imf)}
                ${renderSectors(latest)}
                ${renderMacros(macros)}
            `;
        } catch (error) {
            root.innerHTML = `<p class="error">Unable to load dashboard data: ${esc(error.message)}</p>`;
        }
    }

    init();
})();
