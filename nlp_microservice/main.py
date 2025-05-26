
# Utilities
from data_templates import SIGNAL_CATEGORIES, NOISE_CATEGORIES, TEMPLATES, SYNONYMS

from sklearn.feature_extraction.text import TfidfVectorizer
from fastapi.responses import JSONResponse
from scipy.spatial.distance import cosine
from fastapi import FastAPI, Request
import numpy as np
import random
import logging
import pickle
import re
import os


# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

app = FastAPI()
ALL_LABELS = SIGNAL_CATEGORIES + NOISE_CATEGORIES


class ImprovedTfidfClassifier:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            lowercase=True,
            max_features=5000,
            stop_words='english',
            ngram_range=(1, 2)
        )
        self.label_embeddings = {}
        self.is_fitted = False
    
    def preprocess_text(self, text):
        # Simple preprocessing
        text = text.lower()
        # Remove special chars but keep spaces
        text = re.sub(r'[^\w\s]', '', text)
        return text
    
    def fit(self, texts_by_label):
        """Fit vectorizer with all texts and prepare label embeddings
        
        Args:
            texts_by_label: Dict mapping labels to lists of example texts
        """
        # Flatten all texts for vectorizer training
        all_texts = []
        for texts in texts_by_label.values():
            all_texts.extend(texts)
            
        # Fit vectorizer on all texts
        self.vectorizer.fit(all_texts)
        
        # Create label embeddings by averaging embeddings of their examples
        for label, texts in texts_by_label.items():
            if texts:
                # Preprocess texts
                preprocessed_texts = [self.preprocess_text(text) for text in texts]
                # Get TF-IDF for all examples of this label
                vectors = self.vectorizer.transform(preprocessed_texts).toarray()
                # Average them to get label embedding
                label_vector = vectors.mean(axis=0)
                # Normalize
                norm = np.linalg.norm(label_vector)
                if norm > 0:
                    label_vector = label_vector / norm
                self.label_embeddings[label] = label_vector
                
        self.is_fitted = True
        return self
    
    def get_embedding(self, text):
        """Get TF-IDF embedding for a text"""
        if not self.is_fitted:
            raise ValueError("Vectorizer not fitted")

        processed_text = self.preprocess_text(text)
        vector = self.vectorizer.transform([processed_text])
        embedding = vector.toarray()[0]
        
        # Normalize
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm
            
        return embedding
    
    def classify(self, text, candidate_labels):
        """Calculate similarity between text and label embeddings"""
        text_embedding = self.get_embedding(text)
        scores = []
        
        for label in candidate_labels:
            if label not in self.label_embeddings:
                # Skip labels we don't have embeddings for
                continue
                
            label_embedding = self.label_embeddings[label]
            similarity = 1 - cosine(text_embedding, label_embedding)
            scores.append(similarity)
            
        # Create and sort results
        label_scores = list(zip(candidate_labels, scores))
        label_scores.sort(key=lambda x: x[1], reverse=True)
        
        return {
            "sequence": text,
            "labels": [label for label, _ in label_scores],
            "scores": [float(score) for _, score in label_scores]
        }
        
    
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

# Generates synthetic texts from all the defined templates
def generate_training_texts_by_label():
    texts_by_label = {}
    for label, templates in TEMPLATES.items():
        texts_by_label[label] = []
        for template in templates:
            # Generate multiple variants
            texts_by_label[label].extend([fill_template(template) for _ in range(3)])
    return texts_by_label


# Initialize classifier - either load from file or create new
classifier_path = 'tfidf_classifier.pkl'
if os.path.exists(classifier_path):
    try:
        classifier = ImprovedTfidfClassifier.load(classifier_path)
    except (AttributeError, ModuleNotFoundError, pickle.PicklingError):
        # If loading fails, create a new classifier
        print("Failed to load existing classifier, creating new one...")
        classifier = ImprovedTfidfClassifier()
        texts_by_label = generate_training_texts_by_label()
        classifier.fit(texts_by_label)
        classifier.save(classifier_path)
else:
    # Initialize and train with synthetic data if no saved model exists
    classifier = ImprovedTfidfClassifier()
    texts_by_label = generate_training_texts_by_label()
    classifier.fit(texts_by_label)
    classifier.save(classifier_path)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/classify")
async def classify_message(request: Request):
    results = []
    data = await request.json()
    for msg in data:
        message = msg.get("text", "")
        labels = msg.get("labels", ALL_LABELS)  # default to ALL_LABELS if none provided
        
        if not message:
            results.append({
                "error": "Missing 'text' field",
                "input": msg
            })
            continue
            
        # Pass the message directly to classify
        result = classifier.classify(message, candidate_labels=labels)
        
        try:
            nlp_msg = {
                "message": result["sequence"],
                "category": result["labels"][0],
                "unique_msg_id": msg["unique_msg_id"],
                "macroarea_id": msg["macroarea_id"],
                "microarea_id": msg["microarea_id"],
                "latitude": msg["latitude"],
                "longitude": msg["longitude"],
                "timestamp": msg["timestamp"]
            }
            results.append(nlp_msg)
        except Exception as e:
            print(f"Failed to parse message metadata to nlp output, cause: {e}")
            results.append({
                "error": str(e),
                "input": msg
            })
            
    return JSONResponse(content=results)

