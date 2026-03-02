// Algorithm Deep Dive - Interactive State Visualizations

// BFS State Machine
let bfsStateExecution = {
    step: 0,
    queue: [],
    results: [],
    globalVisited: new Set(),
    currentItem: null,
    autoInterval: null,
    isRunning: false,
    
    // Sample data
    nodes: {
        'u:0': { id: 0, schema: 'users', name: 'alex', age: 25 },
        'f:1': { id: 1, schema: 'users', name: 'bob', age: 31 },
        'c:1': { id: 1, schema: 'companies', name: 'google', size: 3000 }
    },
    
    connections: {
        'u:0': [{ target: 'f:1', edge: 'friend' }],
        'f:1': [{ target: 'c:1', edge: 'works-at' }],
        'c:1': []
    }
};

function initializeBFSState() {
    bfsStateExecution.step = 0;
    bfsStateExecution.queue = [{
        id: 'u:0',
        depth: 0,
        row: {},
        path: [],
        pathVisited: new Set()
    }];
    bfsStateExecution.results = [];
    bfsStateExecution.globalVisited = new Set();
    bfsStateExecution.currentItem = null;
    
    renderBFSState();
}

function stepBFS() {
    const exec = bfsStateExecution;
    
    if (exec.queue.length === 0) {
        renderBFSState('✅ <strong>BFS Complete!</strong> All paths explored.');
        return;
    }
    
    // Dequeue
    exec.currentItem = exec.queue.shift();
    exec.step++;
    
    const node = exec.nodes[exec.currentItem.id];
    const conns = exec.connections[exec.currentItem.id];
    
    // Mark as visited
    exec.globalVisited.add(exec.currentItem.id);
    exec.currentItem.pathVisited.add(exec.currentItem.id);
    
    // Fill row
    for (const key in node) {
        if (key !== 'id' && key !== 'schema') {
            const fieldName = `${node.schema.substring(0, 1)}.${key}`;
            exec.currentItem.row[fieldName] = node[key];
        }
    }
    
    exec.currentItem.path.push(exec.currentItem.id);
    
    // Check for outgoing connections
    if (conns.length === 0) {
        // Complete path - add to results
        exec.results.push({
            ...exec.currentItem.row,
            path: exec.currentItem.path.join(' → ')
        });
        renderBFSState(`<span style="color: var(--success);">✅ Complete path found!</span> Added row to results.`);
    } else {
        // Continue traversal
        conns.forEach(conn => {
            if (!exec.currentItem.pathVisited.has(conn.target)) {
                exec.queue.push({
                    id: conn.target,
                    depth: exec.currentItem.depth + 1,
                    row: { ...exec.currentItem.row },
                    path: [...exec.currentItem.path],
                    pathVisited: new Set(exec.currentItem.pathVisited)
                });
            }
        });
        renderBFSState(`Processing <strong>${exec.currentItem.id}</strong>, enqueued ${conns.length} connection(s).`);
    }
}

function renderBFSState(message = '') {
    const exec = bfsStateExecution;
    const display = document.getElementById('state-display');
    
    if (!display) return;
    
    let html = `
        <div style="color: var(--accent); margin-bottom: 1rem; font-weight: 600;">
            Step ${exec.step}: ${message}
        </div>
        
        <div style="margin-bottom: 1.5rem;">
            <div style="color: var(--primary); font-weight: 600; margin-bottom: 0.5rem;">
                📋 Queue (${exec.queue.length} items):
            </div>
    `;
    
    if (exec.queue.length === 0) {
        html += '<div style="color: var(--text-dim); font-style: italic;">Queue is empty</div>';
    } else {
        exec.queue.forEach((item, idx) => {
            html += `
                <div class="queue-item">
                    <strong>${idx}.</strong> ${item.id} 
                    (depth: ${item.depth}, path: ${item.path.join(' → ') || 'root'})
                </div>
            `;
        });
    }
    
    html += '</div>';
    
    // Current item being processed
    if (exec.currentItem) {
        html += `
            <div style="margin-bottom: 1.5rem;">
                <div style="color: var(--accent); font-weight: 600; margin-bottom: 0.5rem;">
                    ⚡ Currently Processing:
                </div>
                <div class="queue-item" style="border-left-color: var(--accent);">
                    ${exec.currentItem.id} @ depth ${exec.currentItem.depth}<br>
                    Row: ${JSON.stringify(exec.currentItem.row)}<br>
                    Path: ${exec.currentItem.path.join(' → ')}
                </div>
            </div>
        `;
    }
    
    // Global visited
    html += `
        <div style="margin-bottom: 1.5rem;">
            <div style="color: var(--secondary); font-weight: 600; margin-bottom: 0.5rem;">
                ✓ Global Visited (${exec.globalVisited.size}):
            </div>
            <div style="color: var(--text-dim);">
                {${Array.from(exec.globalVisited).join(', ')}}
            </div>
        </div>
    `;
    
    // Results
    html += `
        <div>
            <div style="color: var(--primary); font-weight: 600; margin-bottom: 0.5rem;">
                📊 Results (${exec.results.length} complete rows):
            </div>
    `;
    
    if (exec.results.length === 0) {
        html += '<div style="color: var(--text-dim); font-style: italic;">No complete rows yet</div>';
    } else {
        exec.results.forEach((row, idx) => {
            html += `
                <div style="background: var(--bg-lighter); padding: 0.75rem; margin: 0.5rem 0; border-radius: 4px; border-left: 3px solid var(--success);">
                    <strong>Row ${idx}:</strong><br>
                    ${Object.entries(row).map(([k, v]) => `${k}: ${v}`).join(', ')}
                </div>
            `;
        });
    }
    
    html += '</div>';
    
    display.innerHTML = html;
}

function autoPlayBFS() {
    const exec = bfsStateExecution;
    const btn = document.getElementById('bfs-state-auto');
    
    if (exec.isRunning) {
        clearInterval(exec.autoInterval);
        exec.isRunning = false;
        btn.textContent = '⏯ Auto';
    } else {
        exec.isRunning = true;
        btn.textContent = '⏸ Pause';
        exec.autoInterval = setInterval(() => {
            if (exec.queue.length === 0) {
                clearInterval(exec.autoInterval);
                exec.isRunning = false;
                btn.textContent = '⏯ Auto';
            } else {
                stepBFS();
            }
        }, 1500);
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // BFS State controls
    const stepBtn = document.getElementById('bfs-state-step');
    const resetBtn = document.getElementById('bfs-state-reset');
    const autoBtn = document.getElementById('bfs-state-auto');
    
    if (stepBtn) {
        stepBtn.addEventListener('click', stepBFS);
        resetBtn.addEventListener('click', () => {
            initializeBFSState();
            const autoBtn = document.getElementById('bfs-state-auto');
            if (bfsStateExecution.isRunning) {
                clearInterval(bfsStateExecution.autoInterval);
                bfsStateExecution.isRunning = false;
                autoBtn.textContent = '⏯ Auto';
            }
        });
        autoBtn.addEventListener('click', autoPlayBFS);
        
        // Initialize
        initializeBFSState();
    }
    
    // Initialize MathJax
    if (window.MathJax) {
        MathJax.typesetPromise().catch((err) => console.log('MathJax error:', err));
    }
});


