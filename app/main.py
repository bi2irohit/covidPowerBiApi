from flask import Flask
# import covidMain
from app.covidMain import *

# Initialize application
app = Flask(__name__)
app.config['ENV'] = 'development'
app.config['DEBUG'] = True
app.config['TESTING'] = True

@app.route('/')
def hello():
    return 'This API is working'

@app.route('/covid19PowerBi/world')
def world():
    # jsonData = covidMain.makeWorldData()
    jsonData = makeWorldData()
    return jsonData

@app.route('/covid19PowerBi/country')
def country():
    # jsonData = covidMain.makeCountryData()
    jsonData = makeCountryData()
    return jsonData

@app.route('/covid19PowerBi/india')
def india():
    # jsonData = covidMain.makeIndiaData()
    jsonData = makeIndiaData()
    return jsonData

@app.route('/covid19PowerBi/indiaStates')
def indiaStates():
    # jsonData = covidMain.makeIndiaStateData()
    jsonData = makeIndiaStateData()
    return jsonData            

if __name__ == '__main__':
    # app.run(host = '0.0.0.0',port = 4000, debug = True)
    app.run()