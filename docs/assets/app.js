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
        return `<ul>${(items || []).slice(0, 5).map((n) => {
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

    function renderItem(item) {
        const icons = (item.icons || []).map((icon) => `<span class="item-icon" title="${esc(icon)}">${iconMap[icon] || '•'}</span>`).join('');
        const evidence = (item.insight2 && item.insight2.evidence ? item.insight2.evidence : []).slice(0, 2);
        const evidenceHtml = evidence.length
            ? `<div class="item-evidence">${evidence.map((line) => `<p>${esc(line)}</p>`).join('')}</div>`
            : '';
        const confidence = item.insight2 && item.insight2.confidence ? item.insight2.confidence : '';
        const s1 = item.insight2 && item.insight2.s1 ? item.insight2.s1 : '';
        const s2 = item.insight2 && item.insight2.s2 ? item.insight2.s2 : '';
        const s3 = item.insight2 && item.insight2.s3 ? item.insight2.s3 : '';
        const insightText = (s1 && s2)
            ? `${esc(s1)} ${esc(s2)} ${esc(s3 || 'Why this matters: this development should be monitored for downstream operational effects.')}`
            : 'Open the source for details; summary extraction failed for this item.';

        return `
            <article class="item-card">
                <div class="item-head">
                    <h5><a id="item-${esc(item.id)}"></a><a href="${esc(item.url)}" target="_blank" rel="noopener">${esc(item.title)}</a></h5>
                    <div class="item-icons">${icons}</div>
                </div>
                <p class="item-meta">${esc(item.publisher)} · ${esc(item.publishedAt)} · ${esc(item.sourceTier)}</p>
                <p class="item-insight">${insightText}</p>
                ${evidenceHtml}
                ${confidence ? `<p class="item-confidence">Confidence: ${esc(confidence)}</p>` : ''}
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
            const [latest, highlights, macros] = await Promise.all([
                loadJson('data/latest.json'),
                loadJson('data/highlights.json'),
                loadJson('data/macros.json')
            ]);

            root.innerHTML = `
                <section class="exec-brief panel">
                    <h2>Executive Brief</h2>
                    ${list((highlights.executiveBriefBullets || []).slice(0, 5))}
                </section>
                ${renderTop(highlights)}
                ${renderSectors(latest)}
                ${renderMacros(macros)}
            `;
        } catch (error) {
            root.innerHTML = `<p class="error">Unable to load dashboard data: ${esc(error.message)}</p>`;
        }
    }

    init();
})();
