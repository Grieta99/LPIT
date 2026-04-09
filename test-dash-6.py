
from dash import Dash, html
import dash_ag_grid as dag
import polars as pl

# Incorporate data
df = pl.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv')

# Initialize the app
app = Dash()

# App layout
app.layout = [
    html.Div(children='Dash app with data'),
    dag.AgGrid(
        rowData=df.to_dicts(),
        columnDefs=[{"field": i} for i in df.columns]
    )
]

# Run the app
if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8056, debug=False)

