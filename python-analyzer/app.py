from flask import Flask, request, jsonify
from flask_cors import CORS
from analyzer.text_analyzer import TextAnalyzer

app = Flask(__name__)
CORS(app)

analyzer = TextAnalyzer()

@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        data = request.get_json()
        text = data.get('text', '')
        
        if not text:
            return jsonify({'error': 'No text provided'}), 400
            
        # Perform analysis
        result = analyzer.analyze(text)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 