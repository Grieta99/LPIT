from dash import Dash, html

app = Dash(__name__)
app.layout = html.Div("Hello Dash")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8050, debug=False)    

