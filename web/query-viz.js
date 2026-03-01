// TundraDB Query Execution Visualizations
// Using Cytoscape.js for graph visualizations and D3.js for flowcharts

// ===================================
// 1. Architecture Diagram
// ===================================
function initArchitectureDiagram() {
    const cy = cytoscape({
        container: document.getElementById('architecture-diagram'),
        
        elements: [
            // Nodes
            { data: { id: 'query', label: 'Query\nInput', type: 'input' } },
            { data: { id: 'state', label: 'QueryState', type: 'state' } },
            { data: { id: 'traverse', label: 'Traverse\nPhase', type: 'process' } },
            { data: { id: 'populate', label: 'populate_rows\n(BFS)', type: 'process' } },
            { data: { id: 'arrow', label: 'Arrow\nTable', type: 'output' } },
            { data: { id: 'node_mgr', label: 'NodeManager', type: 'storage' } },
            { data: { id: 'edge_store', label: 'EdgeStore', type: 'storage' } },
            
            // Edges
            { data: { source: 'query', target: 'state', label: 'init' } },
            { data: { source: 'state', target: 'traverse', label: 'process' } },
            { data: { source: 'traverse', target: 'node_mgr', label: 'get nodes' } },
            { data: { source: 'traverse', target: 'edge_store', label: 'get edges' } },
            { data: { source: 'traverse', target: 'state', label: 'update' } },
            { data: { source: 'state', target: 'populate', label: 'materialize' } },
            { data: { source: 'populate', target: 'node_mgr', label: 'read fields' } },
            { data: { source: 'populate', target: 'arrow', label: 'build' } }
        ],
        
        style: [
            {
                selector: 'node',
                style: {
                    'background-color': '#1e293b',
                    'label': 'data(label)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'color': '#f1f5f9',
                    'font-size': '12px',
                    'width': 80,
                    'height': 80,
                    'border-width': 2,
                    'border-color': '#475569',
                    'text-wrap': 'wrap',
                    'text-max-width': 70
                }
            },
            {
                selector: 'node[type="input"]',
                style: {
                    'background-color': '#2563eb',
                    'border-color': '#3b82f6'
                }
            },
            {
                selector: 'node[type="process"]',
                style: {
                    'background-color': '#10b981',
                    'border-color': '#34d399'
                }
            },
            {
                selector: 'node[type="state"]',
                style: {
                    'background-color': '#f59e0b',
                    'border-color': '#fbbf24'
                }
            },
            {
                selector: 'node[type="storage"]',
                style: {
                    'background-color': '#8b5cf6',
                    'border-color': '#a78bfa'
                }
            },
            {
                selector: 'node[type="output"]',
                style: {
                    'background-color': '#ec4899',
                    'border-color': '#f472b6'
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 2,
                    'line-color': '#475569',
                    'target-arrow-color': '#475569',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier',
                    'label': 'data(label)',
                    'font-size': '10px',
                    'color': '#94a3b8',
                    'text-background-color': '#0f172a',
                    'text-background-opacity': 1,
                    'text-background-padding': '3px'
                }
            }
        ],
        
        layout: {
            name: 'breadthfirst',
            directed: true,
            padding: 30,
            spacingFactor: 1.5
        }
    });
    
    // Add animation on hover
    cy.nodes().on('mouseover', function() {
        this.animate({
            style: { 'width': 90, 'height': 90 }
        }, { duration: 200 });
    });
    
    cy.nodes().on('mouseout', function() {
        this.animate({
            style: { 'width': 80, 'height': 80 }
        }, { duration: 200 });
    });
}

// ===================================
// 2. Pipeline Visualization
// ===================================
function initPipelineVisualization() {
    const cy = cytoscape({
        container: document.getElementById('pipeline-viz'),
        
        elements: [
            // Phase 1: Query Input & Parsing
            { data: { id: 'input', label: 'Query\nInput', phase: 1 } },
            { data: { id: 'parse', label: 'Parse &\nBuild', phase: 1 } },
            { data: { id: 'init', label: 'Initialize\nQueryState', phase: 1 } },
            
            // Phase 2: FROM & Loading
            { data: { id: 'from', label: 'FROM\nClause', phase: 2 } },
            { data: { id: 'load', label: 'Load\nTable', phase: 2 } },
            { data: { id: 'extract', label: 'Extract\nIDs', phase: 2 } },
            
            // Phase 3: Traverse
            { data: { id: 'traverse1', label: 'TRAVERSE 1\nu→f', phase: 3 } },
            { data: { id: 'edges1', label: 'Get\nEdges', phase: 3 } },
            { data: { id: 'filter1', label: 'Apply\nFilters', phase: 3 } },
            { data: { id: 'join1', label: 'Join\nLogic', phase: 3 } },
            
            { data: { id: 'traverse2', label: 'TRAVERSE 2\nf→c', phase: 3 } },
            { data: { id: 'edges2', label: 'Get\nEdges', phase: 3 } },
            { data: { id: 'filter2', label: 'Apply\nFilters', phase: 3 } },
            { data: { id: 'join2', label: 'Join\nLogic', phase: 3 } },
            
            // Phase 4: Materialize
            { data: { id: 'populate', label: 'populate_rows\n(BFS)', phase: 4 } },
            { data: { id: 'bfs', label: 'BFS\nTraversal', phase: 4 } },
            { data: { id: 'fill', label: 'Fill\nFields', phase: 4 } },
            
            // Phase 5: Output
            { data: { id: 'build', label: 'Build\nArrow Table', phase: 5 } },
            { data: { id: 'result', label: 'Query\nResult', phase: 5 } },
            
            // Edges - Sequential flow
            { data: { source: 'input', target: 'parse' } },
            { data: { source: 'parse', target: 'init' } },
            { data: { source: 'init', target: 'from' } },
            { data: { source: 'from', target: 'load' } },
            { data: { source: 'load', target: 'extract' } },
            { data: { source: 'extract', target: 'traverse1' } },
            { data: { source: 'traverse1', target: 'edges1' } },
            { data: { source: 'edges1', target: 'filter1' } },
            { data: { source: 'filter1', target: 'join1' } },
            { data: { source: 'join1', target: 'traverse2' } },
            { data: { source: 'traverse2', target: 'edges2' } },
            { data: { source: 'edges2', target: 'filter2' } },
            { data: { source: 'filter2', target: 'join2' } },
            { data: { source: 'join2', target: 'populate' } },
            { data: { source: 'populate', target: 'bfs' } },
            { data: { source: 'bfs', target: 'fill' } },
            { data: { source: 'fill', target: 'build' } },
            { data: { source: 'build', target: 'result' } }
        ],
        
        style: [
            {
                selector: 'node',
                style: {
                    'background-color': '#334155',
                    'label': 'data(label)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'color': '#f1f5f9',
                    'font-size': '11px',
                    'width': 70,
                    'height': 70,
                    'border-width': 3,
                    'border-color': '#64748b',
                    'text-wrap': 'wrap',
                    'text-max-width': 60
                }
            },
            {
                selector: 'node[phase="1"]',
                style: {
                    'background-color': '#2563eb',
                    'border-color': '#3b82f6'
                }
            },
            {
                selector: 'node[phase="2"]',
                style: {
                    'background-color': '#8b5cf6',
                    'border-color': '#a78bfa'
                }
            },
            {
                selector: 'node[phase="3"]',
                style: {
                    'background-color': '#10b981',
                    'border-color': '#34d399'
                }
            },
            {
                selector: 'node[phase="4"]',
                style: {
                    'background-color': '#f59e0b',
                    'border-color': '#fbbf24'
                }
            },
            {
                selector: 'node[phase="5"]',
                style: {
                    'background-color': '#ec4899',
                    'border-color': '#f472b6'
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 3,
                    'line-color': '#475569',
                    'target-arrow-color': '#475569',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier'
                }
            }
        ],
        
        layout: {
            name: 'breadthfirst',
            directed: true,
            padding: 20,
            spacingFactor: 1.2,
            avoidOverlap: true
        }
    });
    
    // Add hover animation
    cy.nodes().on('mouseover', function() {
        this.animate({
            style: { 
                'width': 80, 
                'height': 80,
                'border-width': 5
            }
        }, { duration: 200 });
    });
    
    cy.nodes().on('mouseout', function() {
        this.animate({
            style: { 
                'width': 70, 
                'height': 70,
                'border-width': 3
            }
        }, { duration: 200 });
    });
}

// ===================================
// 3. Traverse Graph Visualization
// ===================================
function initTraverseGraph() {
    const cy = cytoscape({
        container: document.getElementById('traverse-graph'),
        
        elements: [
            // User nodes
            { data: { id: 'u0', label: 'Alex\nu:0', schema: 'users' } },
            { data: { id: 'u1', label: 'Bob\nu:1', schema: 'users' } },
            { data: { id: 'u2', label: 'Jeff\nu:2', schema: 'users' } },
            
            // Company nodes
            { data: { id: 'c0', label: 'IBM\nc:0', schema: 'companies' } },
            { data: { id: 'c1', label: 'Google\nc:1', schema: 'companies' } },
            { data: { id: 'c2', label: 'AWS\nc:2', schema: 'companies' } },
            
            // Edges
            { data: { source: 'u0', target: 'u1', label: 'friend', matched: true } },
            { data: { source: 'u1', target: 'c1', label: 'works-at', matched: true } }
        ],
        
        style: [
            {
                selector: 'node',
                style: {
                    'background-color': '#334155',
                    'label': 'data(label)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'color': '#f1f5f9',
                    'font-size': '12px',
                    'width': 60,
                    'height': 60,
                    'border-width': 3,
                    'border-color': '#64748b',
                    'text-wrap': 'wrap'
                }
            },
            {
                selector: 'node[schema="users"]',
                style: {
                    'background-color': '#2563eb',
                    'border-color': '#3b82f6'
                }
            },
            {
                selector: 'node[schema="companies"]',
                style: {
                    'background-color': '#10b981',
                    'border-color': '#34d399'
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 3,
                    'line-color': '#475569',
                    'target-arrow-color': '#475569',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier',
                    'label': 'data(label)',
                    'font-size': '11px',
                    'color': '#cbd5e1',
                    'text-background-color': '#0f172a',
                    'text-background-opacity': 1
                }
            },
            {
                selector: 'edge[matched]',
                style: {
                    'line-color': '#f59e0b',
                    'target-arrow-color': '#f59e0b',
                    'width': 4
                }
            }
        ],
        
        layout: {
            name: 'cose',
            padding: 30,
            nodeRepulsion: 8000,
            idealEdgeLength: 100
        }
    });
}

// ===================================
// 4. BFS Visualization with Animation
// ===================================
let bfsState = {
    step: 0,
    queue: [],
    visited: new Set(),
    currentRow: {},
    results: [],
    autoPlay: false,
    interval: null
};

function initBFSVisualization() {
    const cy = cytoscape({
        container: document.getElementById('bfs-viz'),
        
        elements: [
            { data: { id: 'u0', label: 'Alex\nu:0', depth: 0 } },
            { data: { id: 'f1', label: 'Bob\nf:1', depth: 1 } },
            { data: { id: 'c1', label: 'Google\nc:1', depth: 2 } },
            { data: { source: 'u0', target: 'f1' } },
            { data: { source: 'f1', target: 'c1' } }
        ],
        
        style: [
            {
                selector: 'node',
                style: {
                    'background-color': '#334155',
                    'label': 'data(label)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'color': '#f1f5f9',
                    'font-size': '14px',
                    'width': 70,
                    'height': 70,
                    'border-width': 3,
                    'border-color': '#64748b',
                    'text-wrap': 'wrap'
                }
            },
            {
                selector: 'node.current',
                style: {
                    'background-color': '#f59e0b',
                    'border-color': '#fbbf24',
                    'border-width': 5
                }
            },
            {
                selector: 'node.visited',
                style: {
                    'background-color': '#10b981',
                    'border-color': '#34d399'
                }
            },
            {
                selector: 'node.queued',
                style: {
                    'background-color': '#2563eb',
                    'border-color': '#3b82f6'
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 3,
                    'line-color': '#475569',
                    'target-arrow-color': '#475569',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier'
                }
            },
            {
                selector: 'edge.active',
                style: {
                    'line-color': '#f59e0b',
                    'target-arrow-color': '#f59e0b',
                    'width': 5
                }
            }
        ],
        
        layout: {
            name: 'breadthfirst',
            directed: true,
            padding: 30
        }
    });
    
    // BFS Controls
    document.getElementById('bfs-step').addEventListener('click', () => bfsStep(cy));
    document.getElementById('bfs-reset').addEventListener('click', () => bfsReset(cy));
    document.getElementById('bfs-auto').addEventListener('click', () => bfsAutoPlay(cy));
    
    // Initialize BFS
    bfsReset(cy);
}

function bfsStep(cy) {
    if (bfsState.queue.length === 0) {
        updateBFSState('BFS Complete! All paths explored.');
        return;
    }
    
    const current = bfsState.queue.shift();
    bfsState.visited.add(current.id);
    
    // Update visualization
    cy.nodes().removeClass('current');
    cy.getElementById(current.id).addClass('current visited');
    
    // Simulate filling row fields
    bfsState.currentRow[current.id] = `{...fields from ${current.id}}`;
    
    // Get neighbors
    const neighbors = cy.getElementById(current.id).outgoers('node').filter(n => !bfsState.visited.has(n.id()));
    
    if (neighbors.length === 0) {
        // Leaf node - add to results
        bfsState.results.push({...bfsState.currentRow});
        updateBFSState(`✅ Complete row: ${JSON.stringify(bfsState.currentRow)}`);
    } else {
        // Enqueue neighbors
        neighbors.forEach(n => {
            if (!bfsState.queue.find(q => q.id === n.id())) {
                bfsState.queue.push({ id: n.id(), depth: current.depth + 1 });
                n.addClass('queued');
                cy.edges(`#${current.id}[target="${n.id()}"]`).addClass('active');
            }
        });
        updateBFSState(`Processing ${current.id}, enqueued ${neighbors.length} neighbors`);
    }
    
    bfsState.step++;
}

function bfsReset(cy) {
    bfsState = {
        step: 0,
        queue: [{ id: 'u0', depth: 0 }],
        visited: new Set(),
        currentRow: {},
        results: [],
        autoPlay: false
    };
    
    if (bfsState.interval) {
        clearInterval(bfsState.interval);
        bfsState.interval = null;
    }
    
    cy.nodes().removeClass('current visited queued');
    cy.edges().removeClass('active');
    cy.getElementById('u0').addClass('queued');
    
    updateBFSState('BFS initialized. Queue: [u:0]');
}

function bfsAutoPlay(cy) {
    if (bfsState.autoPlay) {
        clearInterval(bfsState.interval);
        bfsState.autoPlay = false;
        document.getElementById('bfs-auto').textContent = 'Auto Play';
    } else {
        bfsState.autoPlay = true;
        document.getElementById('bfs-auto').textContent = 'Pause';
        bfsState.interval = setInterval(() => {
            if (bfsState.queue.length === 0) {
                clearInterval(bfsState.interval);
                bfsState.autoPlay = false;
                document.getElementById('bfs-auto').textContent = 'Auto Play';
            } else {
                bfsStep(cy);
            }
        }, 1500);
    }
}

function updateBFSState(message) {
    const stateDiv = document.getElementById('bfs-state');
    stateDiv.innerHTML = `
        <div><strong>Step ${bfsState.step}:</strong> ${message}</div>
        <div style="margin-top: 0.5rem;">Queue: [${bfsState.queue.map(q => q.id).join(', ')}]</div>
        <div>Visited: {${Array.from(bfsState.visited).join(', ')}}</div>
        <div>Results: ${bfsState.results.length} complete rows</div>
    `;
}

// ===================================
// 5. Interactive Demo
// ===================================
function initInteractiveDemo() {
    const queries = {
        inner: {
            graph: [
                { data: { id: 'u0', label: 'Alex', matched: true } },
                { data: { id: 'u1', label: 'Bob', matched: true } },
                { data: { id: 'u2', label: 'Jeff', matched: false } },
                { data: { source: 'u0', target: 'u1', label: 'friend' } }
            ],
            log: [
                '1. FROM u:users → ids={0,1,2}',
                '2. TRAVERSE u-[friend]->f',
                '   u:0 → f:1 ✓ matched',
                '   u:2 → none ✗ unmatched (removed)',
                '3. INNER JOIN: ids[u]={0}, ids[f]={1}',
                '4. populate_rows: 1 complete row'
            ],
            table: [
                ['u.id', 'u.name', 'f.id', 'f.name'],
                ['0', 'Alex', '1', 'Bob']
            ]
        },
        left: {
            graph: [
                { data: { id: 'u0', label: 'Alex', matched: true } },
                { data: { id: 'u2', label: 'Jeff', matched: true } },
                { data: { id: 'u1', label: 'Bob', matched: true } },
                { data: { source: 'u0', target: 'u1', label: 'friend' } }
            ],
            log: [
                '1. FROM u:users → ids={0,1,2}',
                '2. TRAVERSE u-[friend LEFT]->f',
                '   u:0 → f:1 ✓ matched',
                '   u:2 → none ✓ kept (LEFT JOIN)',
                '3. LEFT JOIN: ids[u]={0,2}, ids[f]={1}',
                '4. populate_rows: 2 rows (1 with NULL)'
            ],
            table: [
                ['u.id', 'u.name', 'f.id', 'f.name'],
                ['0', 'Alex', '1', 'Bob'],
                ['2', 'Jeff', 'NULL', 'NULL']
            ]
        },
        full: {
            graph: [
                { data: { id: 'u0', label: 'Alex' } },
                { data: { id: 'u1', label: 'Bob' } },
                { data: { id: 'u2', label: 'Jeff' } },
                { data: { source: 'u0', target: 'u1', label: 'friend' } }
            ],
            log: [
                '1. FROM u:users → ids={0,1,2}',
                '2. TRAVERSE u-[friend FULL]->f',
                '3. Self-join: unmatched = all - matched_sources',
                '4. FULL JOIN: All sources + all unmatched targets',
                '5. populate_rows: 4 rows total'
            ],
            table: [
                ['u.id', 'u.name', 'f.id', 'f.name'],
                ['0', 'Alex', '1', 'Bob'],
                ['1', 'Bob', 'NULL', 'NULL'],
                ['2', 'Jeff', 'NULL', 'NULL'],
                ['NULL', 'NULL', '2', 'Jeff']
            ]
        },
        multi: {
            graph: [
                { data: { id: 'u0', label: 'Alex' } },
                { data: { id: 'f1', label: 'Bob' } },
                { data: { id: 'c1', label: 'Google' } },
                { data: { source: 'u0', target: 'f1', label: 'friend' } },
                { data: { source: 'f1', target: 'c1', label: 'works-at' } }
            ],
            log: [
                '1. FROM u:users → ids={0}',
                '2. TRAVERSE u-[friend]->f → ids[f]={1}',
                '3. TRAVERSE f-[works-at]->c → ids[c]={1}',
                '4. BFS: u:0 → f:1 → c:1',
                '5. Complete path in single row'
            ],
            table: [
                ['u.id', 'u.name', 'f.id', 'f.name', 'c.id', 'c.name'],
                ['0', 'Alex', '1', 'Bob', '1', 'Google']
            ]
        }
    };
    
    let demoCy = null;
    
    document.getElementById('run-query').addEventListener('click', () => {
        const selected = document.getElementById('query-select').value;
        const queryData = queries[selected];
        
        // Clear previous
        document.getElementById('demo-graph-viz').innerHTML = '';
        
        // Create new graph
        demoCy = cytoscape({
            container: document.getElementById('demo-graph-viz'),
            elements: queryData.graph,
            style: [
                {
                    selector: 'node',
                    style: {
                        'background-color': '#2563eb',
                        'label': 'data(label)',
                        'text-valign': 'center',
                        'text-halign': 'center',
                        'color': '#f1f5f9',
                        'font-size': '12px',
                        'width': 50,
                        'height': 50,
                        'border-width': 2,
                        'border-color': '#3b82f6'
                    }
                },
                {
                    selector: 'node[matched]',
                    style: {
                        'background-color': '#10b981',
                        'border-color': '#34d399'
                    }
                },
                {
                    selector: 'node[matched="false"]',
                    style: {
                        'background-color': '#ef4444',
                        'border-color': '#f87171'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 2,
                        'line-color': '#f59e0b',
                        'target-arrow-color': '#f59e0b',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier',
                        'label': 'data(label)',
                        'font-size': '10px',
                        'color': '#fbbf24'
                    }
                }
            ],
            layout: {
                name: 'cose',
                padding: 20
            }
        });
        
        // Update log
        document.getElementById('demo-log').innerHTML = queryData.log.map(line => 
            `<div style="margin-bottom: 0.5rem;">${line}</div>`
        ).join('');
        
        // Update table
        const table = document.createElement('table');
        queryData.table.forEach((row, i) => {
            const tr = document.createElement('tr');
            row.forEach(cell => {
                const td = document.createElement(i === 0 ? 'th' : 'td');
                td.textContent = cell;
                tr.appendChild(td);
            });
            table.appendChild(tr);
        });
        document.getElementById('demo-table').innerHTML = '';
        document.getElementById('demo-table').appendChild(table);
    });
}

// ===================================
// 6. Join Type Visualizations
// ===================================
function initJoinVisualizations() {
    const joins = ['inner', 'left', 'right', 'full'];
    
    joins.forEach(type => {
        const elements = getJoinElements(type);
        cytoscape({
            container: document.getElementById(`join-${type}`),
            elements: elements,
            style: [
                {
                    selector: 'node',
                    style: {
                        'background-color': '#334155',
                        'label': 'data(label)',
                        'text-valign': 'center',
                        'color': '#f1f5f9',
                        'font-size': '11px',
                        'width': 40,
                        'height': 40,
                        'border-width': 2,
                        'border-color': '#64748b'
                    }
                },
                {
                    selector: 'node.included',
                    style: {
                        'background-color': '#10b981',
                        'border-color': '#34d399'
                    }
                },
                {
                    selector: 'node.excluded',
                    style: {
                        'background-color': '#ef4444',
                        'border-color': '#f87171'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 2,
                        'line-color': '#475569',
                        'target-arrow-color': '#475569',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier'
                    }
                }
            ],
            layout: {
                name: 'grid',
                padding: 10
            }
        });
    });
}

function getJoinElements(type) {
    const base = [
        { data: { id: 'u0', label: 'Alex' }, classes: 'included' },
        { data: { id: 'u1', label: 'Bob' }, classes: type === 'inner' ? 'excluded' : 'included' },
        { data: { id: 'f1', label: 'Bob' }, classes: 'included' },
        { data: { source: 'u0', target: 'f1' } }
    ];
    
    if (type === 'right' || type === 'full') {
        base.push({ data: { id: 'f2', label: 'Jeff' }, classes: 'included' });
    }
    
    return base;
}

// ===================================
// Initialize Everything
// ===================================
document.addEventListener('DOMContentLoaded', () => {
    initArchitectureDiagram();
    initPipelineVisualization();
    initTraverseGraph();
    initBFSVisualization();
    initInteractiveDemo();
    initJoinVisualizations();
});

