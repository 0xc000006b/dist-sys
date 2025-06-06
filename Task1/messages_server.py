from flask import Flask

app = Flask(__name__)

@app.route('/messages', methods=['GET'])
def get_message():
    return "not implemented yet", 200, {'Content-Type': 'text/plain; charset=utf-8'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
