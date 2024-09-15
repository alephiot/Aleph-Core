from flask import Flask, render_template, request, redirect, url_for

from aleph.models.record_set import RecordSet
from aleph.connections.mqtt import MqttConnection

from example.constants import Namespace


app = Flask(__name__)

mqtt_connection = MqttConnection("1234")
mqtt_connection.open()


@app.route("/")
def index():
    return "Index page"


@app.route("/messages")
def messages_list():
    batches = mqtt_connection.read(Namespace.BATCHES)
    persons = mqtt_connection.read(Namespace.PERSONS)
    return str(batches) + "<br>" + str(persons)


if __name__ == "__main__":
    app.run(debug=True)
