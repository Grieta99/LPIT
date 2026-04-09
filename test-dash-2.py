from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import polars as pl

# Create Polars DataFrame
df = pl.DataFrame({
    "x": [1, 2, 3, 4, 5],
    "A": [10, 15, 13, 17, 20],
    "B": [7, 11, 14, 9, 18]
})

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Dash + Polars Example"),
    
    dcc.Dropdown(
        id="series-dropdown",
        options=[
            {"label": "Series A", "value": "A"},
            {"label": "Series B", "value": "B"},
        ],
        value="A",
        clearable=False
    ),
    
    dcc.Graph(id="line-chart")
])

@app.callback(
    Output("line-chart", "figure"),
    Input("series-dropdown", "value")
)
def update_chart(selected_series):
    # Convert Polars columns to Python lists
    x = df["x"].to_list()
    y = df[selected_series].to_list()

    fig = px.line(
        x=x,
        y=y,
        markers=True,
        title=f"Showing {selected_series}"
    )
    return fig

if __name__ == "__main__":
    app.run(debug=True)