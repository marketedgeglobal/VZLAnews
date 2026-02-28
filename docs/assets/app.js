(function () {
    async function loadJson(path) {
        const response = await fetch(path, { cache: 'no-store' });
        if (!response.ok) throw new Error('Failed to load ' + path);
        return response.json();
    }

    function esc(value) {
        return String(value || '').replace(/[&<>"']/g, (char) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char] || char
        ));
    }

    function detectLanguage(item) {
        const declared = (item && item.language ? String(item.language) : '').toLowerCase();
        return (declared === 'es' || declared === 'en') ? declared : 'other';
    }

    function normalizePreview(text) {
        const clean = String(text || '').replace(/\s+/g, ' ').trim();
        if (!clean || clean.includes('…') || clean.includes('...')) return '';
        const parts = clean.split(/(?<=[.!?])\s+(?=[A-ZÁÉÍÓÚÑ])/).map((s) => s.trim()).filter(Boolean);
        if (parts.length < 2) return '';
        const two = `${parts[0]} ${parts[1]}`.trim();
        if (two.length > 340) return '';
        return two;
    }

    function isArticleUrl(rawUrl) {
        if (!rawUrl) return false;
        let parsed;
        try {
            parsed = new URL(rawUrl);
        } catch {
            return false;
        }
        const path = (parsed.pathname || '').toLowerCase();
        const badExact = new Set(['', '/', '/en', '/es', '/news', '/en/news', '/en/news/', '/rss', '/rss.xml', '/feed', '/feeds', '/home']);
        if (badExact.has(path)) return false;
        const badStarts = ['/rss', '/feed', '/feeds', '/topic/', '/topics/', '/category/', '/categories/', '/country/', '/countries/', '/about', '/search', '/sitemap'];
        if (badStarts.some((prefix) => path.startsWith(prefix))) return false;
        const segments = path.split('/').filter(Boolean);
        if (segments.length < 2) return false;
        const hasDate = /\b20\d{2}\/(0?[1-9]|1[0-2])\/(0?[1-9]|[12]\d|3[01])\b/.test(path)
            || /\b20\d{2}-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])\b/.test(path);
        const last = segments[segments.length - 1] || '';
        const hasLongSlug = last.length >= 12 && !last.endsWith('.xml');
        const goodPrefixes = ['/publication', '/publications', '/report', '/reports', '/document', '/documents', '/press-release', '/press-releases', '/news/story', '/news/feature', '/resources', '/library'];
        const hasGoodPrefix = goodPrefixes.some((prefix) => path.startsWith(prefix));
        return hasDate || hasLongSlug || hasGoodPrefix;
    }

    function renderLanguageSwitcher(activeLanguage) {
        return `
            <section class="panel language-panel">
                <div class="language-switch" role="group" aria-label="Language filter">
                    <button class="lang-btn ${activeLanguage === 'en' ? 'active' : ''}" data-lang="en" type="button">English</button>
                    <button class="lang-btn ${activeLanguage === 'es' ? 'active' : ''}" data-lang="es" type="button">Español</button>
                </div>
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
        const preview = normalizePreview(item.preview || '');
        if (preview.length < 80) return '';
        const sourceDate = (item.sourcePublishedAt || '').trim();
        const isVerified = sourceDate.length > 0;

        return `
            <article class="item-card">
                <div class="item-head">
                    <h5><a id="item-${esc(item.id)}"></a><a href="${esc(item.url)}" target="_blank" rel="noopener">${esc(item.title)}</a></h5>
                </div>
                ${isVerified ? '<p class="item-verified">Verified article URL</p>' : ''}
                ${sourceDate ? `<p class="item-source-date">Source date: ${esc(sourceDate)}</p>` : ''}
                <p class="item-desc">${esc(preview)}</p>
            </article>
        `;
    }

    function renderSectors(latest, activeLanguage, rejectedRuntime) {
        return (latest.sectors || []).map((sector) => {
            const renderedItems = (sector.items || [])
                .filter((item) => {
                    if (detectLanguage(item) !== activeLanguage) {
                        rejectedRuntime.push({ reason: 'wrong_language', title: item.title || '', finalUrl: item.url || '' });
                        return false;
                    }
                    if (!isArticleUrl(item.url || '')) {
                        rejectedRuntime.push({ reason: 'url_not_article_runtime', title: item.title || '', finalUrl: item.url || '' });
                        return false;
                    }
                    return true;
                })
                .map(renderItem)
                .filter(Boolean)
                .join('');
            if (!renderedItems) return '';
            return `
                <section class="sector-block">
                    <h3>${esc(sector.name)}</h3>
                    <div class="items-grid">${renderedItems}</div>
                </section>
            `;
        }).filter(Boolean).join('');
    }

    function renderRejectedDebug(rejectedRuntime, rejectedBuild) {
        const merged = [...(rejectedRuntime || []), ...(rejectedBuild || [])];
        if (!merged.length) return '';
        const rows = merged.slice(0, 300).map((item) => `
            <li><strong>${esc(item.reason || 'rejected')}</strong> — ${esc(item.title || '')}${item.finalUrl ? ` · <a href="${esc(item.finalUrl)}" target="_blank" rel="noopener">link</a>` : ''}</li>
        `).join('');
        return `
            <section class="panel">
                <details>
                    <summary>Rejected items (debug)</summary>
                    <ul>${rows}</ul>
                </details>
            </section>
        `;
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
            const [latest, macros, imf] = await Promise.all([
                loadJson('data/latest.json'),
                loadJson('data/macros.json'),
                loadIMF()
            ]);
            const debugMode = new URLSearchParams(window.location.search).get('debug') === '1';
            let rejectedBuild = [];
            if (debugMode) {
                try {
                    rejectedBuild = await loadJson('data/rejected_links.json');
                } catch {
                    rejectedBuild = [];
                }
            }

            let activeLanguage = 'en';
            const languageCounts = (latest.sectors || []).flatMap((sector) => (sector.items || []))
                .reduce((acc, item) => {
                    const lang = detectLanguage(item);
                    if (lang === 'en' || lang === 'es') acc[lang] += 1;
                    return acc;
                }, { en: 0, es: 0 });
            if (languageCounts.en === 0 && languageCounts.es > 0) activeLanguage = 'es';

            const render = () => {
                const rejectedRuntime = [];
                const sectorsHtml = renderSectors(latest, activeLanguage, rejectedRuntime);
                root.innerHTML = `
                    ${renderLanguageSwitcher(activeLanguage)}
                    ${renderIMFCard(imf)}
                    ${sectorsHtml || '<section class="panel"><p>No article previews available for the selected language.</p></section>'}
                    ${renderMacros(macros)}
                    ${debugMode ? renderRejectedDebug(rejectedRuntime, rejectedBuild) : ''}
                `;
            };

            root.addEventListener('click', (event) => {
                const target = event.target;
                if (!(target instanceof HTMLElement)) return;
                if (!target.classList.contains('lang-btn')) return;
                const selected = target.getAttribute('data-lang');
                if (!selected || (selected !== 'en' && selected !== 'es') || selected === activeLanguage) return;
                activeLanguage = selected;
                render();
            });

            render();
        } catch (error) {
            root.innerHTML = `<p class="error">Unable to load dashboard data: ${esc(error.message)}</p>`;
        }
    }

    init();
})();
