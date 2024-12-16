import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from typing import List, Optional
import os

class CarbonAIMistral:
    def __init__(self, model_name: str = "mistralai/Mistral-7B-v0.1", token: str = None):
        if token is None:
            token = os.getenv('HF_TOKEN')
            if token is None:
                raise ValueError("Please provide a Hugging Face token either through the constructor or by setting the HF_TOKEN environment variable")
        
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {self.device}")
        
        # Load tokenizer and model
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, token=token)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            token=token,
            torch_dtype=torch.float16 if self.device == "cuda" else torch.float32,
            low_cpu_mem_usage=True,
            device_map="auto"
        )
        
        # Load carbon knowledge base
        self.carbon_knowledge = self._load_carbon_knowledge()
        
    def _load_carbon_knowledge(self) -> str:
        """Load the carbon knowledge base from carbon.txt"""
        try:
            with open("carbon.txt", "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            print(f"Warning: Could not load carbon.txt: {e}")
            return ""
            
    def generate_response(self, query: str, max_length: int = 500) -> str:
        """Generate a response about carbon-related topics"""
        # Create a context-aware prompt
        prompt = f"""Based on scientific knowledge about carbon and its properties:
        
Knowledge Context:
{self.carbon_knowledge[:2000]}  # Use first 2000 chars of knowledge base

Question: {query}

Please provide a detailed, scientifically accurate response:"""

        # Tokenize input
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        
        # Generate response
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_length=max_length,
                num_return_sequences=1,
                temperature=0.7,
                top_p=0.9,
                do_sample=True,
                pad_token_id=self.tokenizer.eos_token_id
            )
        
        # Decode and return the response
        response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # Extract only the generated part (after the prompt)
        response = response[len(prompt):]
        return response.strip()

    def get_carbon_opinion(self, topic: str) -> str:
        """Get an informed opinion about a carbon-related topic"""
        query = f"What is the relationship between carbon and {topic}? Please provide scientific insights and implications."
        return self.generate_response(query)

# Example usage
if __name__ == "__main__":
    # Initialize the model
    carbon_ai = CarbonAIMistral()
    
    # Test with a sample question
    sample_topics = [
        "climate change",
        "diamond formation",
        "organic chemistry",
        "carbon dating"
    ]
    
    print("Carbon AI Model Test:")
    print("-" * 50)
    for topic in sample_topics:
        print(f"\nTopic: {topic}")
        response = carbon_ai.get_carbon_opinion(topic)
        print(f"Response: {response}\n")
        print("-" * 50)


