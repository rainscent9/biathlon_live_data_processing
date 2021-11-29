from kafka import KafkaConsumer
import json
import dash
from dash.dependencies import Output, Input
from dash import dcc
from dash import html
import plotly
import plotly.graph_objs as go

## SPEED VISUALISATION

# topic names
hrv_topic = 'hrv_with_level_bpm_s'
speed_topic = 'gps_for_vis_s'

# set topic to read from
topic_name = hrv_topic
# Set variables to read from topic (x and y axis in graph)
x_variable = 'SPEED_ROLLING_MEAN'
y_variable = 'TIMESTAMP'

bootstrap_server = '86.119.35.55:9092'

## define consumer of kafk topic
consumer = KafkaConsumer(speed_topic,
                         auto_offset_reset='earliest',   ## start reading at beginning
                         bootstrap_servers=[bootstrap_server], ## server to connect to
                         value_deserializer=lambda x: x.decode('utf-8') ##normalise
                        )

# function to get values from topic
def get_value():
    for msg in consumer:
        message = json.loads(msg.value) ## convert message value to dict

        if message[x_variable] is not None:  ## due to many None values, skip them
            yield (message[x_variable], message[y_variable]) ## return tuple with x and y values


## APP

app = dash.Dash(__name__)

server = app.server
app.layout = html.Div(
    ## HTMl for grpah
    [
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=3000,
            n_intervals = 0
        ),
    ]
)
## emptly list for x & y values
x_values = []
y_values = []

# decorator to update graph, callback functions updates graph
@app.callback(Output('live-graph', 'figure'),
              [Input('graph-update', 'n_intervals')])

# graph itself
def update_graph(n):
   x_value, y_value = next(get_value())  ## get values frojm function
   x_values.append(x_value)
   y_values.append(y_value)

   data = plotly.graph_objs.Scatter(
       x=x_values,
       y=y_values,
       name='Scatter',
       mode='lines+markers'
   )

   return {'data': [data],
            'layout': go.Layout(xaxis=dict(range=[min(x_values), max(x_values)]),
                                yaxis=dict(range=[min(y_values), max(y_values)]),
                                title='Biathlete data')}


## run app
if __name__ == "__main__":
    app.run_server()
