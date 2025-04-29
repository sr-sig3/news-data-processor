from konlpy.tag import Okt
from collections import Counter
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

class TextAnalyzer:
    def __init__(self):
        self.okt = Okt()
        nltk.download('vader_lexicon')
        self.sia = SentimentIntensityAnalyzer()

    def analyze(self, text):
        # Tokenize and extract nouns
        nouns = self.okt.nouns(text)
        
        # Get word frequencies
        word_freq = Counter(nouns)
        
        # Perform sentiment analysis
        sentiment = self.sia.polarity_scores(text)
        
        # Extract key phrases (simple implementation)
        key_phrases = self._extract_key_phrases(text)
        
        return {
            'word_frequencies': dict(word_freq.most_common(10)),
            'sentiment': sentiment,
            'key_phrases': key_phrases
        }
    
    def _extract_key_phrases(self, text):
        # Simple implementation - can be enhanced with more sophisticated algorithms
        pos = self.okt.pos(text)
        phrases = []
        current_phrase = []
        
        for word, tag in pos:
            if tag in ['Noun', 'Adjective']:
                current_phrase.append(word)
            else:
                if current_phrase:
                    phrases.append(' '.join(current_phrase))
                    current_phrase = []
        
        if current_phrase:
            phrases.append(' '.join(current_phrase))
            
        return phrases[:5]  # Return top 5 phrases 