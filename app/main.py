from flask import Flask
import covidMain

# Initialize application
app = Flask(__name__)

@app.route('/')
def hello():
    return 'This API is working'

@app.route('/world')
def world():
    jsonData = covidMain.makeWorldData()
    return jsonData

@app.route('/country')
def country():
    jsonData = covidMain.makeCountryData()
    return jsonData

@app.route('/india')
def india():
    jsonData = covidMain.makeIndiaData()
    return jsonData

@app.route('/indiaStates')
def indiaStates():
    jsonData = covidMain.makeIndiaStateData()
    return jsonData            

if __name__ == '__main__':
    app.run(port = 8282)