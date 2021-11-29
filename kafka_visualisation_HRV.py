from kafka import KafkaConsumer
import json
import dash
from dash.dependencies import Output, Input
from dash import dcc
from dash import html
import plotly
import plotly.graph_objs as go

## VISUALISATION OF HRV_ROLLING_MEAN

# topic names
hrv_topic = 'hrv_with_level_bpm_s'
speed_topic = 'gps_for_vis_s'

# set topic to read form
topic_name = hrv_topic
# Set variables to read from topic
x_variable = 'HRV_ROLLING_MEAN'
y_variable = 'TIMESTAMP'


bootstrap_server = '86.119.35.55:9092'

## Define Kafka consumer, to read from topic
consumer = KafkaConsumer(topic_name,
                         auto_offset_reset='earliest',   ## start reading at beginning
                         bootstrap_servers=[bootstrap_server],
                         value_deserializer=lambda x: x.decode('utf-8')
                        )

## function to get values from Kafka consumer
def get_value():
    for msg in consumer:
        message = json.loads(msg.value)

        if message[x_variable] is not None:  # return only values that are not None
            yield (message[x_variable], message[y_variable])



## APP
app = dash.Dash(__name__)

server = app.server
app.layout = html.Div(
    [
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1000,
            n_intervals = 0
        ),
    ]
)

## empty list to append incoming values to
x_values = []
y_values = []

# decorator to update graph
@app.callback(Output('live-graph', 'figure'),
              [Input('graph-update', 'n_intervals')])

# graph parameters
def update_graph(n):
   x_value, y_value = next(get_value())
   x_values.append(x_value)
   y_values.append(y_value)

   data = plotly.graph_objs.Scatter(
       x=x_values,
       y=y_values,
       name='Scatter',
       mode='lines+markers')

   return {'data': [data],
            'layout': go.Layout(xaxis=dict(range=[min(x_values), max(x_values)]),
                                yaxis=dict(range=[min(y_values), max(y_values)]),
                                title='Biathlete data')}


## start app
if __name__ == "__main__":
    app.run_server()
