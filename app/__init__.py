from flask import Flask
# import covidMain
import app.covidMain

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
    jsonData = covidMain.makeWorldData()
    return jsonData

# @app.route('/covid19PowerBi/country')
# def country():
#     jsonData = covidMain.makeCountryData()
#     return jsonData

# @app.route('/covid19PowerBi/india')
# def india():
#     jsonData = covidMain.makeIndiaData()
#     return jsonData

# @app.route('/covid19PowerBi/indiaStates')
# def indiaStates():
#     jsonData = covidMain.makeIndiaStateData()
#     return jsonData            

if __name__ == '__main__':
    # app.run(host = '0.0.0.0',port = 4000, debug = True)
    app.run()