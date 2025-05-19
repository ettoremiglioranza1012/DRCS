
# Utilities
from data_templates import SIGNAL_CATEGORIES, NOISE_CATEGORIES, TEMPLATES, SYNONYMS

from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.spatial.distance import cosine
from fastapi import FastAPI, Request
import numpy as np
import random
import logging
import pickle
import re


# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

app = FastAPI()
ALL_LABELS = SIGNAL_CATEGORIES + NOISE_CATEGORIES


class SimpleTfidfClassifier:
    def __init__(self):
        # Initialize TF-IDF vectorizer
        self.vectorizer = TfidfVectorizer(
            lowercase=True,
            max_features=5000,  # Limit vocabulary size
            stop_words='english',  # Remove common words
            ngram_range=(1, 2)  # Use unigrams and bigrams
        )
        
        # Pre-compute label embeddings
        self.label_embeddings = {}
        self.is_fitted = False
    
    def preprocess_text(self, text):
        # Simple preprocessing
        text = text.lower()
        # Remove special chars but keep spaces
        text = re.sub(r'[^\w\s]', '', text)
        return text
    
    def fit(self, texts):
        """Fit the vectorizer with some sample texts"""
        preprocessed_texts = [self.preprocess_text(text) for text in texts]
        self.vectorizer.fit(preprocessed_texts)
        self.is_fitted = True
        return self
    
    def get_embedding(self, text):
        """Get TF-IDF embedding for a text"""
        if not self.is_fitted:
            raise ValueError("Vectorizer not fitted. Call fit() with sample texts first.")
        
        preprocessed = self.preprocess_text(text)
        vector = self.vectorizer.transform([preprocessed])
        # Convert sparse vector to dense and normalize
        embedding = vector.toarray()[0]
        # Avoid division by zero
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm
        return embedding
    
    def cache_label_embedding(self, label):
        """Cache embedding for a label"""
        if label not in self.label_embeddings:
            self.label_embeddings[label] = self.get_embedding(label)
        return self.label_embeddings[label]
    
    def classify(self, text, candidate_labels):
        """Calculate similarity between text and candidate labels"""
        # Get text embedding
        text_embedding = self.get_embedding(text)
        
        scores = []
        for label in candidate_labels:
            # Get or compute label embedding
            label_embedding = self.cache_label_embedding(label)
            
            # Calculate similarity (1 - cosine distance)
            # Handle zero vectors
            if np.sum(label_embedding) == 0 or np.sum(text_embedding) == 0:
                similarity = 0.0
            else:
                similarity = 1 - cosine(text_embedding, label_embedding)
            
            scores.append(similarity)
        
        # Convert to list of (label, score) pairs
        label_scores = list(zip(candidate_labels, scores))
        
        # Sort by score in descending order
        label_scores.sort(key=lambda x: x[1], reverse=True)
        
        result = {
            "sequence": text,
            "labels": [label for label, _ in label_scores],
            "scores": [float(score) for _, score in label_scores]
        }
        
        return result
    
    def save(self, filepath):
        """Save the classifier to a file"""
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls, filepath):
        """Load the classifier from a file"""
        with open(filepath, 'rb') as f:
            return pickle.load(f)

def fill_template(template: str) -> str:
    return template.format(**{
        key: random.choice(values)
        for key, values in SYNONYMS.items()
        if f"{{{key}}}" in template
    })

# Genera testi sintetici da tutti i template definiti
def generate_training_texts():
    samples = []
    for category_templates in TEMPLATES.values():
        for template in category_templates:
            # Genera più varianti di ogni template per migliorare la copertura
            samples.extend([fill_template(template) for _ in range(3)])
    return samples

SAMPLE_TEXTS = generate_training_texts()

# Initialize classifier
try:
    # Try to load from file if exists
    classifier = SimpleTfidfClassifier.load('tfidf_classifier.pkl')
    logging.info("Model loaded from file")
except:
    # Or create and fit a new one
    classifier = SimpleTfidfClassifier().fit(SAMPLE_TEXTS)
    classifier.save('tfidf_classifier.pkl')
    logging.info("New model created and saved")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/classify")
async def classify_message(request: Request):
    data = await request.json()
    message = data.get("message", "")
    labels = data.get("labels", ALL_LABELS)  # default to ALL_LABELS if none provided

    if not message:
        return {"error": "Provide a 'message' in the request body."}

    result = classifier.classify(message, candidate_labels=labels)

    return {"message": result["sequence"], "category": result["labels"][0]}


@app.post("/train")
async def train_model(request: Request):
    """Endpoint to add more training texts"""
    data = await request.json()
    texts = data.get("texts", [])
    
    if not texts or not isinstance(texts, list):
        return {"error": "Provide a list of texts in the 'texts' field"}
    
    # Update the model with new texts
    classifier.fit(texts)
    classifier.save('tfidf_classifier.pkl')
    
    return {"status": "success", "message": f"Model updated with {len(texts)} new texts"}

