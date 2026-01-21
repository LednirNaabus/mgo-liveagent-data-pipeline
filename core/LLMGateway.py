"""
LLM Gateway using LiteLLM for model fallback support.
"""
from config.config import OPENAI_API_KEY, GEMINI_API_KEY
from typing import Dict, List, Any, Optional
import litellm
import logging
import asyncio
import json
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class LLMGateway:
    """
    LLM Gateway that uses LiteLLM to manage multiple LLM providers
    with automatic fallback support.
    """
    
    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        gemini_api_key: Optional[str] = None,
        temperature: float = 0.8
    ):
        self.temperature = temperature
        
        self.openai_api_key = openai_api_key or OPENAI_API_KEY
        self.gemini_api_key = gemini_api_key or GEMINI_API_KEY
        
        if self.openai_api_key:
            os.environ["OPENAI_API_KEY"] = self.openai_api_key
        if self.gemini_api_key:
            os.environ["GEMINI_API_KEY"] = self.gemini_api_key
        
        self.fallback_models = [
            "gpt-4o-mini",
            "gemini/gemini-2.5-flash"
        ]
        
        if not self.openai_api_key and not self.gemini_api_key:
            raise ValueError(
                "At least one API key (OPENAI_API_KEY or GEMINI_API_KEY) must be provided"
            )
        
        logging.info("LLM Gateway initialized with fallback models:")
        for idx, model in enumerate(self.fallback_models, 1):
            logging.info(f"  {idx}. {model}")
    
    async def completion(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Any] = None,
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Make a completion request with automatic fallback using litellm.responses API.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            response_format: Pydantic model for structured output (required)
            model: Optional specific model to use (overrides fallback)
        
        Returns:
            Dict containing the response data and metadata
        """
        if not response_format:
            raise ValueError("response_format (Pydantic model) is required")
        
        models_to_try = [model] if model else self.fallback_models
        
        last_error = None
        
        for current_model in models_to_try:
            try:
                logging.info(f"Attempting completion with model: {current_model}")
                
                loop = asyncio.get_event_loop()
                
                response = await loop.run_in_executor(
                    None,
                    lambda: litellm.completion(
                        model=current_model,
                        messages=messages,
                        temperature=self.temperature,
                        response_format=response_format
                    )
                )
                
                logging.info(f"Successfully completed with model: {current_model}")
                
                content = response.choices[0].message.content
                
                try:
                    parsed_data = json.loads(content)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON response: {e}")
                    logging.error(f"Raw content: {content}")
                    raise
                
                usage = response.usage if hasattr(response, 'usage') else None
                total_tokens = usage.total_tokens if usage else 0
                prompt_tokens = usage.prompt_tokens if usage else 0
                completion_tokens = usage.completion_tokens if usage else 0
                
                actual_model = response.model if hasattr(response, 'model') else current_model
                
                return {
                    "content": content,
                    "model": actual_model,
                    "tokens": total_tokens,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens
                }
                
            except Exception as e:
                last_error = e
                logging.warning(
                    f"Model {current_model} failed: {str(e)}"
                )
                
                if current_model != models_to_try[-1]:
                    logging.info(f"Falling back to next model...")
                    continue
                else:
                    logging.error(
                        f"All models failed. Last error: {str(last_error)}"
                    )
                    raise RuntimeError(
                        f"All LLM models failed. Last error: {str(last_error)}"
                    ) from last_error
        
        raise RuntimeError("Unexpected error in LLM Gateway")
    
    def get_available_models(self) -> List[str]:
        """Return list of configured fallback models."""
        return self.fallback_models.copy()