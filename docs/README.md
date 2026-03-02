# TundraDB Interactive Documentation

**🎮 Live, interactive HTML documentation with animated visualizations**

## 📋 What's Included

This folder contains a complete **interactive website** for TundraDB's query execution algorithm:

### Files

- **`index.html`** - Main documentation page with sections:
  - Overview with feature cards
  - Architecture diagram (Cytoscape.js)
  - Query execution pipeline
  - Traverse & join logic
  - BFS algorithm with step-by-step animation
  - Interactive demo with multiple query examples
  - Join type comparisons (INNER, LEFT, RIGHT, FULL)
  - Performance characteristics

- **`algorithm-deep-dive.html`** - **NEW!** Mathematical deep dive:
  - **LaTeX/MathJax** rendering for formulas
  - Formal mathematical definitions
  - **5 detailed phases** with theory and implementation
  - Algorithmic invariants
  - Interactive BFS state explorer
  - Complexity analysis tables
  - Real execution traces
  - Self-join vs cross-schema with ID collision examples

- **`query-execution.html`** - Live demo page:
  - Real-time graph visualization
  - Step-by-step query execution
  - Preset queries (INNER, LEFT, FULL, Multi-Hop)
  - Execution log
  - Result table display
  - Auto-play mode

- **`styles.css`** - Modern, dark-themed styling:
  - Responsive design
  - Smooth animations
  - Card-based layouts
  - Professional color scheme

- **`query-viz.js`** - JavaScript for interactive visualizations:
  - Architecture diagram
  - Graph traversal animations
  - BFS visualization with controls
  - Join type demonstrations
  - Interactive demo with query execution

## 🚀 How to View

### Option 1: Simple HTTP Server (Recommended)

```bash
# From the web directory
cd /Users/dmgcodevil/dev/cpp/tundradb-v1/tundradb/web

# Python 3
python3 -m http.server 8000

# Then open: http://localhost:8000
```

### Option 2: Open Directly

```bash
# macOS
open index.html

# Linux
xdg-open index.html
```

**Note:** Some features (like external resources) work better with a local server.

## 🎨 Features

### 1. Architecture Visualization
- **Cytoscape.js** graph showing component interactions
- Node types: Input, Process, State, Storage, Output
- Animated hover effects
- Color-coded by component type

### 2. BFS Animation
- **Step-by-step** breadth-first search visualization
- Controls: Step, Reset, Auto-play
- Shows queue state, visited nodes, and results
- Real-time execution log

### 3. Interactive Demo
- **4 preset queries**:
  - Simple INNER JOIN
  - LEFT JOIN with NULL
  - FULL OUTER JOIN
  - Multi-Hop Path
- Live graph updates
- Execution log with detailed steps
- Result table display

### 4. Join Type Comparisons
- Visual examples of all 4 join types
- Self-join vs cross-schema explained
- Color-coded included/excluded nodes
- SQL-like syntax examples

### 5. Live Query Execution
- Separate `query-execution.html` page
- **Interactive controls**:
  - Step through execution
  - Auto-play mode
  - Reset
- Progress bar
- Real-time query state
- Result table

## 🎮 Navigation

### Main Page (index.html)
- **Sidebar navigation** for quick access to sections
- Smooth scroll animations
- "Live Demo" button in sidebar → opens `query-execution.html`
- Sections:
  - Overview
  - Pipeline
  - Traverse & Joins
  - BFS
  - Demo
  - Joins
  - Performance

### Live Demo Page (query-execution.html)
- **Three-column layout**:
  - Left: Query builder + execution log
  - Center: Graph visualization
  - Right: Query state + results
- **Controls**:
  - Preset query buttons
  - Step-by-step execution
  - Auto-play with pause
  - Progress indicator

## 📊 Technologies Used

### JavaScript Libraries
- **Cytoscape.js** (v3.28.1) - Graph visualizations
  - Node connections
  - Edge traversal
  - Layout algorithms (cose, breadthfirst, grid)
  
- **D3.js** (v7) - Data visualizations
  - Flowcharts
  - Animations
  
- **Mermaid.js** (v10) - Diagram rendering
  - State diagrams
  - Flowcharts

### CSS Features
- **CSS Grid** for layouts
- **CSS Animations** for transitions
- **CSS Variables** for theming
- **Flexbox** for components

### Fonts
- **Inter** - UI text (sans-serif)
- **Fira Code** - Code blocks (monospace)

## 🎨 Visual Design

### Color Palette
- **Background**: Dark blue-gray (`#0f172a`, `#1e293b`)
- **Primary**: Blue (`#2563eb`)
- **Secondary**: Green (`#10b981`)
- **Accent**: Amber (`#f59e0b`)
- **Text**: Light gray (`#f1f5f9`)
- **Borders**: Gray (`#475569`)

### Node Colors
- **Input**: Blue (`#2563eb`)
- **Process**: Green (`#10b981`)
- **State**: Amber (`#f59e0b`)
- **Storage**: Purple (`#8b5cf6`)
- **Output**: Pink (`#ec4899`)
- **Active**: Amber (`#f59e0b`)
- **Excluded**: Red (`#ef4444`)

## 📱 Responsive Design

- **Desktop** (> 1024px): Three-column layout
- **Tablet** (768px - 1024px): Two-column layout
- **Mobile** (< 768px): Single-column stacked layout

## 🔧 Customization

### Add New Query Examples

Edit `query-viz.js`:

```javascript
const queries = {
    your_query: {
        graph: [...],  // Cytoscape elements
        log: [...],    // Execution steps
        table: [...]   // Result data
    }
};
```

### Change Colors

Edit `styles.css`:

```css
:root {
    --primary: #your-color;
    --secondary: #your-color;
    ...
}
```

### Add New Sections

Edit `index.html`:

```html
<section id="your-section" class="section">
    <h1>Your Title</h1>
    <p>Your content...</p>
</section>
```

Update sidebar navigation:

```html
<li><a href="#your-section">Your Section</a></li>
```

## 📖 Documentation Structure

```
web/
├── index.html              # Main documentation page
├── query-execution.html    # Live demo page
├── styles.css              # Global styles
├── query-viz.js            # Visualization logic
└── README.md               # This file
```

## 🐛 Troubleshooting

### Visualizations Not Showing
- Ensure you're using a local server (not `file://`)
- Check browser console for errors
- Verify CDN links are loading (internet required)

### Layout Issues
- Try a different browser (Chrome/Firefox recommended)
- Check browser zoom level (100% recommended)
- Clear browser cache

### Performance Issues
- Reduce number of nodes in graph
- Disable animations in CSS
- Close other browser tabs

## 🚀 Deployment

### To GitHub Pages

1. Push to `gh-pages` branch:
```bash
git checkout -b gh-pages
git add web/
git commit -m "Add interactive docs"
git push origin gh-pages
```

2. Enable GitHub Pages in repo settings
3. Access at: `https://username.github.io/tundradb/web/`

### To Static Hosting

1. Upload `web/` folder to:
   - Netlify
   - Vercel
   - AWS S3
   - Any static host

2. No build step required!

## 📊 Features Matrix

| Feature | Status | Description |
|---------|--------|-------------|
| Architecture Diagram | ✅ | Component interaction graph |
| Pipeline Visualization | ✅ | Query execution flow |
| Traverse Animation | ✅ | Edge traversal with joins |
| BFS Animation | ✅ | Step-by-step with controls |
| Interactive Demo | ✅ | 4 preset queries |
| Live Execution | ✅ | Real-time step-through |
| Join Comparisons | ✅ | All 4 join types |
| Performance Metrics | ✅ | Complexity tables |
| Responsive Design | ✅ | Mobile-friendly |
| Dark Theme | ✅ | Professional styling |

## 🎯 Next Enhancements

Potential additions:
- [ ] Query syntax highlighting
- [ ] Export visualizations as PNG/SVG
- [ ] More preset queries
- [ ] Temporal query examples
- [ ] Performance profiling visualizations
- [ ] 3D graph visualization (Three.js)
- [ ] WebGL-accelerated rendering for large graphs

## 📝 Credits

- **Cytoscape.js**: https://js.cytoscape.org/
- **D3.js**: https://d3js.org/
- **Mermaid**: https://mermaid.js.org/
- **Inter Font**: https://rsms.me/inter/
- **Fira Code**: https://github.com/tonsky/FiraCode

## 📧 Support

For questions or issues:
- Refer to main [TundraDB README](../README.md)
- Check [QUERY_EXECUTION_ALGORITHM.md](../QUERY_EXECUTION_ALGORITHM.md) for algorithm details
- See [DOCUMENTATION_INDEX.md](../DOCUMENTATION_INDEX.md) for all docs

---

**Enjoy exploring TundraDB's query execution engine!** 🚀

**View the live demo:** Open `index.html` in your browser or run:
```bash
python3 -m http.server 8000
```
Then navigate to `http://localhost:8000`

