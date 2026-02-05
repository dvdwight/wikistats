"""
Wikidata network visualization dashboard.

Interactive Streamlit app for exploring the Wikidata entity relationships.
"""

import streamlit as st
import duckdb
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import json

st.set_page_config(
    page_title="WikiStats - Entity Network",
    page_icon="🌐",
    layout="wide"
)

st.title("🌐 Wikidata Entity Network Explorer")

# Initialize connection
@st.cache_resource
def get_db_connection():
    # Use absolute path to warehouse
    warehouse_path = Path(__file__).parent / "warehouse" / "dev.duckdb"
    return duckdb.connect(str(warehouse_path), read_only=True)

conn = get_db_connection()

# Load data
@st.cache_data
def load_entities():
    query = """
        SELECT 
            qid,
            label,
            description,
            instance_of,
            subclass_of,
            first_seen_ingestion,
            last_updated
        FROM dim_entities_clean
    """
    df = conn.execute(query).fetch_df()
    entities = {}
    for _, row in df.iterrows():
        entities[row['qid']] = {
            'label': row['label'],
            'description': row['description'],
            'instance_of': row['instance_of'],
            'subclass_of': row['subclass_of'],
            'first_seen': row['first_seen_ingestion'],
            'last_updated': row['last_updated']
        }
    return entities

@st.cache_data
def load_edges():
    query = """
        SELECT 
            source_qid,
            target_qid,
            relationship_type,
            source_label,
            target_label
        FROM fct_edges_clean
    """
    return conn.execute(query).fetch_df()

@st.cache_data
def load_article_stats():
    """Load article counts by QID, wiki type, and region"""
    query = """
        SELECT 
            wikidata_id as qid,
            wiki,
            COUNT(*) as article_count
        FROM stg_wikistats_enriched
        GROUP BY wikidata_id, wiki
    """
    return conn.execute(query).fetch_df()

# Load data
entities = load_entities()
edges_df = load_edges()
article_stats = load_article_stats()

# Build graph
@st.cache_data
def build_graph():
    G = nx.DiGraph()
    for qid, entity in entities.items():
        G.add_node(qid, label=entity['label'], first_seen=entity['first_seen'])
    
    for _, row in edges_df.iterrows():
        G.add_edge(
            row['source_qid'],
            row['target_qid'],
            relationship=row['relationship_type']
        )
    return G

G = build_graph()

# Sidebar controls
st.sidebar.header("⚙️ Controls")

# Filter by relationship type
rel_types = edges_df['relationship_type'].unique().tolist()
selected_rels = st.sidebar.multiselect(
    "Relationship types:",
    rel_types,
    default=rel_types
)

# Search for entity
search_query = st.sidebar.text_input("🔍 Search entities:", placeholder="e.g., 'human', 'Q5'")

# Graph size control
num_nodes = st.sidebar.slider(
    "Max nodes to display:",
    10, 500, 100, step=10
)

# Create tabs
tab1, tab2 = st.tabs(["🕸️ Network Visualization", "📊 Data Table"])

# Filter edges and nodes
filtered_edges = edges_df[edges_df['relationship_type'].isin(selected_rels)]

if search_query:
    # Filter entities by search
    matching_qids = {
        qid for qid, entity in entities.items()
        if search_query.lower() in entity['label'].lower() or search_query.upper() in qid
    }
    # Include related entities
    related_qids = set()
    for qid in matching_qids:
        related_qids.update(filtered_edges[filtered_edges['source_qid'] == qid]['target_qid'].tolist())
        related_qids.update(filtered_edges[filtered_edges['target_qid'] == qid]['source_qid'].tolist())
    
    display_qids = matching_qids | related_qids
else:
    # Show highly connected nodes
    node_degrees = dict(G.degree())
    display_qids = set(sorted(node_degrees, key=node_degrees.get, reverse=True)[:num_nodes])

# Filter edges to display
display_edges = filtered_edges[
    (filtered_edges['source_qid'].isin(display_qids)) &
    (filtered_edges['target_qid'].isin(display_qids))
]

# Build visualization graph
G_vis = nx.DiGraph()
for qid in display_qids:
    if qid in entities:
        G_vis.add_node(
            qid,
            label=entities[qid]['label'],
            first_seen=entities[qid]['first_seen']
        )

for _, row in display_edges.iterrows():
    G_vis.add_edge(
        row['source_qid'],
        row['target_qid'],
        relationship=row['relationship_type']
    )

# Compute layout
pos = nx.spring_layout(G_vis, k=2, iterations=50, seed=42)

# Determine node colors based on first_seen date
def get_color_for_date(date_str):
    """Return color based on ingestion date (newer = warmer)"""
    if not date_str:
        return 'rgb(150, 150, 150)'  # Gray for unknown
    
    try:
        date = datetime.fromisoformat(date_str)
        today = datetime.now()
        days_ago = (today - date).days
        
        # Gradient: recent = red, older = blue
        if days_ago <= 1:
            return 'rgb(255, 0, 0)'  # Red for today
        elif days_ago <= 3:
            return 'rgb(255, 100, 0)'  # Orange for recent
        elif days_ago <= 7:
            return 'rgb(255, 255, 0)'  # Yellow for week
        else:
            return 'rgb(0, 100, 255)'  # Blue for older
    except:
        return 'rgb(150, 150, 150)'

# Create Plotly figure
edge_x = []
edge_y = []
for edge in G_vis.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.append(x0)
    edge_x.append(x1)
    edge_x.append(None)
    edge_y.append(y0)
    edge_y.append(y1)
    edge_y.append(None)

edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    mode='lines',
    line=dict(width=0.5, color='rgba(100, 100, 100, 0.5)'),
    hoverinfo='none',
    showlegend=False
)

node_x = []
node_y = []
node_text = []
node_color = []
node_size = []

for node in G_vis.nodes():
    x, y = pos[node]
    node_x.append(x)
    node_y.append(y)
    
    entity = entities.get(node, {})
    label = entity.get('label', node)
    desc = entity.get('description', 'No description')
    
    hover_text = f"<b>{label}</b> ({node})<br><sub>{desc}</sub>"
    node_text.append(hover_text)
    node_color.append(get_color_for_date(entity.get('first_seen')))
    
    # Size based on degree
    node_size.append(10 + G_vis.degree(node) * 2)

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers+text',
    text=[entities.get(node, {}).get('label', node)[:10] for node in G_vis.nodes()],
    textposition='top center',
    textfont=dict(size=8),
    hoverinfo='text',
    hovertext=node_text,
    marker=dict(
        size=node_size,
        color=node_color,
        line_width=1,
        line=dict(color='white')
    ),
    showlegend=False
)

fig = go.Figure(data=[edge_trace, node_trace])
fig.update_layout(
    title_text="Entity Relationship Network",
    showlegend=False,
    hovermode='closest',
    margin=dict(b=20, l=5, r=5, t=40),
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    height=700
)

with tab1:
    st.plotly_chart(fig, use_container_width='stretch')
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Nodes Displayed", len(G_vis.nodes()))
    with col2:
        st.metric("Edges Displayed", len(G_vis.edges()))
    with col3:
        st.metric("Total Nodes", len(entities))
    with col4:
        st.metric("Graph Density", f"{nx.density(G_vis):.3f}")

with tab2:
    st.header("📊 Entity Data & Analytics")
    
    # Create tabs within tab2
    data_tab, viz_tab = st.tabs(["📋 Table", "📈 Analytics"])
    
    with data_tab:
        st.subheader("Entity Data Table")
        st.info("ℹ️ **Note:** This table shows cleaned entities (human-readable, no Wikimedia items). Article counts are only shown for entities that were directly referenced in Wikipedia articles during enrichment. Most entities shown here are referenced indirectly through relationships.", icon="ℹ️")
        
        # Load all entities as dataframe with wiki counts
        entity_data = []
        for qid, entity in entities.items():
            # Parse instance_of if it's JSON
            instance_of_list = []
            if entity['instance_of']:
                try:
                    if isinstance(entity['instance_of'], str):
                        instance_of_list = json.loads(entity['instance_of'])
                    elif isinstance(entity['instance_of'], list):
                        instance_of_list = entity['instance_of']
                except:
                    instance_of_list = []
            
            # Convert QIDs to labels for display
            type_labels = []
            for type_qid in (instance_of_list or [])[:3]:  # Show up to 3 types
                if type_qid and type_qid in entities:
                    label = entities[type_qid]['label']
                    # Skip Q## formatted labels (stubs)
                    if not (isinstance(label, str) and label and label[0] == 'Q' and label[1:].isdigit()):
                        type_labels.append(label)
                elif type_qid and not (isinstance(type_qid, str) and type_qid and type_qid[0] == 'Q' and type_qid[1:].isdigit()):
                    # Only add QID if it's not in Q## format
                    type_labels.append(type_qid)
            
            # Count articles for this entity
            qid_articles = article_stats[article_stats['qid'] == qid]
            article_count = qid_articles['article_count'].sum() if len(qid_articles) > 0 else 0
            # Most common wiki for this entity (if any articles exist)
            top_wiki = qid_articles.nlargest(1, 'article_count')['wiki'].values[0] if len(qid_articles) > 0 else '—'
            
            entity_data.append({
                'QID': qid,
                'Label': entity['label'],
                'Description': entity['description'][:100] if entity['description'] else '',
                'Wikidata Type': ' > '.join(type_labels) if type_labels else 'N/A',
                'Top Wiki': top_wiki,
                'Article Count': article_count,
                'First Seen': entity['first_seen'],
            })
        
        df_entities = pd.DataFrame(entity_data)
        
        # Get available wiki types for filtering
        available_wikis = sorted(article_stats['wiki'].unique().tolist())
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            search_text = st.text_input("🔍 Search:", placeholder="human, Q5...")
        
        with col2:
            selected_wikis = st.multiselect(
                "Filter by Wiki:",
                available_wikis,
                help="enwiki=English, frwiki=French, wikidatawiki=Wikidata, etc."
            )
        
        with col3:
            wikidata_types = sorted([t for t in df_entities['Wikidata Type'].unique() if t != 'N/A'])
            selected_types = st.multiselect(
                "Filter by Type:",
                wikidata_types,
                help="Wikidata classification/type"
            )
        
        with col4:
            sort_by = st.selectbox(
                "Sort by:",
                ["Label (A-Z)", "Articles (Most)", "Articles (Least)", "First Seen (Recent)"]
            )
        
        # Apply filters
        df_filtered = df_entities.copy()
        
        if search_text:
            df_filtered = df_filtered[
                (df_filtered['Label'].str.contains(search_text, case=False, na=False)) |
                (df_filtered['QID'].str.contains(search_text.upper(), na=False))
            ]
        
        if selected_wikis:
            df_filtered = df_filtered[df_filtered['Top Wiki'].isin(selected_wikis)]
        
        if selected_types:
            df_filtered = df_filtered[
                df_filtered['Wikidata Type'].apply(
                    lambda x: any(t in x for t in selected_types) if x != 'N/A' else False
                )
            ]
        
        # Apply sorting
        if sort_by == "Label (A-Z)":
            df_filtered = df_filtered.sort_values('Label')
        elif sort_by == "Articles (Most)":
            df_filtered = df_filtered.sort_values('Article Count', ascending=False)
        elif sort_by == "Articles (Least)":
            df_filtered = df_filtered.sort_values('Article Count')
        elif sort_by == "First Seen (Recent)":
            df_filtered = df_filtered.sort_values('First Seen', ascending=False)
        
        # Display table
        st.write(f"**Showing {len(df_filtered)} of {len(df_entities)} entities**")
        st.dataframe(
            df_filtered,
            use_container_width=True,
            hide_index=True,
            column_config={
                "QID": st.column_config.TextColumn(width="small"),
                "Label": st.column_config.TextColumn(width="medium"),
                "Description": st.column_config.TextColumn(width="large"),
                "Wikidata Type": st.column_config.TextColumn(width="medium"),
                "Top Wiki": st.column_config.TextColumn(width="small"),
                "Article Count": st.column_config.NumberColumn(width="small"),
                "First Seen": st.column_config.TextColumn(width="small"),
            }
        )
    
    with viz_tab:
        st.subheader("Article Distribution Analytics")
        
        # Wiki distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Articles by Wiki Type**")
            wiki_counts = article_stats.groupby('wiki')['article_count'].sum().reset_index()
            wiki_counts = wiki_counts.sort_values('article_count', ascending=False)
            
            fig_wiki = go.Figure(data=[
                go.Bar(
                    y=wiki_counts['wiki'],
                    x=wiki_counts['article_count'],
                    orientation='h',
                    marker_color='steelblue'
                )
            ])
            fig_wiki.update_layout(
                title="Total Articles by Wiki",
                xaxis_title="Count",
                yaxis_title="Wiki",
                height=400,
                showlegend=False
            )
            st.plotly_chart(fig_wiki, use_container_width=True)
        
        with col2:
            st.write("**Top 10 Entities by Article Count**")
            top_entities = df_entities.nlargest(10, 'Article Count')[['Label', 'Article Count']]
            
            fig_entities = go.Figure(data=[
                go.Bar(
                    x=top_entities['Label'],
                    y=top_entities['Article Count'],
                    marker_color='coral'
                )
            ])
            fig_entities.update_layout(
                title="Top Entities",
                xaxis_title="Entity",
                yaxis_title="Article Count",
                height=400,
                xaxis_tickangle=-45,
                showlegend=False
            )
            st.plotly_chart(fig_entities, use_container_width=True)
        
        # Summary stats
        st.write("---")
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        
        with stat_col1:
            total_articles = article_stats['article_count'].sum()
            st.metric("Total Articles", f"{total_articles:,}")
        
        with stat_col2:
            num_wikis = article_stats['wiki'].nunique()
            st.metric("Number of Wikis", num_wikis)
        
        with stat_col3:
            articles_per_entity = article_stats.groupby('qid')['article_count'].sum().mean()
            st.metric("Avg Articles/Entity", f"{articles_per_entity:.1f}")
        
        with stat_col4:
            entities_with_articles = article_stats['qid'].nunique()
            st.metric("Entities with Articles", entities_with_articles)

# Entity details (still in sidebar, shared across tabs)
st.sidebar.header("📋 Selected Entity Details")
selected_qid = st.sidebar.selectbox(
    "Choose an entity to inspect:",
    sorted(display_qids),
    format_func=lambda qid: f"{entities.get(qid, {}).get('label', qid)} ({qid})"
)

if selected_qid in entities:
    entity = entities[selected_qid]
    st.sidebar.write(f"**Label:** {entity['label']}")
    st.sidebar.write(f"**QID:** {selected_qid}")
    st.sidebar.write(f"**Description:** {entity['description']}")
    st.sidebar.write(f"**First Seen:** {entity['first_seen']}")
    st.sidebar.write(f"**Last Updated:** {entity['last_updated']}")
    
    # Show related entities
    incoming = filtered_edges[filtered_edges['target_qid'] == selected_qid]
    outgoing = filtered_edges[filtered_edges['source_qid'] == selected_qid]
    
    if len(incoming) > 0:
        st.sidebar.write("**← Incoming (is instance/subclass of):**")
        for _, row in incoming.head(5).iterrows():
            st.sidebar.write(f"  - {row['source_label']} ({row['relationship_type']})")
    
    if len(outgoing) > 0:
        st.sidebar.write("**→ Outgoing (has instance/subclass):**")
        for _, row in outgoing.head(5).iterrows():
            st.sidebar.write(f"  - {row['target_label']} ({row['relationship_type']})")

# Legend
st.markdown("""
---
### 🎨 Legend
- **Node Color**: Based on ingestion date
  - 🔴 Red = Today
  - 🟠 Orange = Last 3 days
  - 🟡 Yellow = Last week
  - 🔵 Blue = Older
- **Node Size**: Based on connectivity (degree)
- **Edges**: Show relationships (instance_of, subclass_of)
""")
