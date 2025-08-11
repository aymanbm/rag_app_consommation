from dotenv import load_dotenv
import os
from langchain_ollama import OllamaLLM

load_dotenv()


MODEL_NAME = os.getenv("MODEL_NAME")

def initialize_llm_model():
    try:
        llmModel = OllamaLLM(model=MODEL_NAME, temperature=0.1)
    except Exception as e:
        print("LLM init failed:", e)
        llmModel = None
    return llmModel