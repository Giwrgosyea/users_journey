import json
import dash
import dash_core_components as dcc
import dash_html_components as html
from viz_helper.generate_plotly import *


##### Data and Predictions #####


visited=get_visited_page()
transactions=get_transactions()
most_popular=get_most_popular_page()
user_spend=get_user_spend()
table_full=get_table()
# Prepare Boxplot for sentiment scores
fig1 = generate_plotly_viz(
    visited, "bar", "Most Site Visited",yaxis='visited')
fig2 = generate_plotly_viz(
    most_popular, "bar", "Site Popularity",yaxis='popularity')
fig3 = generate_plotly_viz(
    transactions, "bar", "Most Transactions per Page",yaxis='transactions')
fig6 = generate_plotly_viz(
    table_full, "table", "Steps to Purchase")
fig4 = generate_plotly_viz_indicator(
    user_spend)
fig5 = generate_grouped(
    visited,most_popular,transactions,'Grouped Bar Chart')

##### Dashboard layout #####
# Dash Set up
app = dash.Dash()

# Base Layout
app.layout = html.Div([
    html.Div([html.H1('Site Review Analysis')],
             style={'width': '90%', 'margin': 'auto', 'text-align': 'center'}
             ),  # Headline
    html.Div(
        dcc.Graph(
            id='plt',
            figure=fig1
        )
    ),  # plot
    html.Div(
        dcc.Graph(
            id='plt1',
            figure=fig2
        )
    ),  # plot
    html.Div(
        dcc.Graph(
            id='plt2',
            figure=fig3
        )
    ),  # plot
    html.Div(
        dcc.Graph(
            id='plt4',
            figure=fig5
        )
    ),  # plot
    html.Div(
        dcc.Graph(
            id='plt3',
            figure=fig4
        )
    ),  # plot
    html.Div(
        dcc.Graph(
            id='plt5',
            figure=fig6
        )
    ),  # plot

])  # End base Div

def render_table(tab):
    pass


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=9000)
