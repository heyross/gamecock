"""Ollama API handler for managing Ollama LLM interactions."""
import httpx
import json
import psutil
from typing import Optional, Dict, Any
from loguru import logger

class OllamaHandler:
    """Handler for Ollama API operations."""
    
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "mistral:latest"):
        """Initialize Ollama handler."""
        self.base_url = base_url
        self.model = model
        self.config = self._get_default_config()
        
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration based on system specs."""
        try:
            # Get system specs
            cpu_count = psutil.cpu_count(logical=False)  # Physical cores only
            total_ram = psutil.virtual_memory().total / (1024 * 1024 * 1024)  # GB
            
            # Calculate optimal parameters
            num_ctx = min(8192, int(total_ram * 256))  # Roughly estimate max context
            num_thread = max(1, min(cpu_count - 1, 4))  # Leave one core free
            
            return {
                "name": self.model,
                "parameters": {
                    "num_ctx": num_ctx,
                    "num_thread": num_thread,
                    "temperature": 0.7,
                    "top_k": 40,
                    "top_p": 0.9,
                    "repeat_penalty": 1.1
                }
            }
        except Exception as e:
            logger.warning(f"Error getting system specs: {str(e)}. Using conservative defaults.")
            return {
                "name": self.model,
                "parameters": {
                    "num_ctx": 4096,
                    "num_thread": 2,
                    "temperature": 0.7,
                    "top_k": 40,
                    "top_p": 0.9,
                    "repeat_penalty": 1.1
                }
            }
            
    def is_running(self) -> bool:
        """Check if Ollama service is running."""
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=5.0)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error checking Ollama service: {str(e)}")
            return False
            
    def is_model_available(self) -> bool:
        """Check if required model is available."""
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=5.0)
            if response.status_code != 200:
                return False
                
            data = response.json()
            model_names = [model['name'] for model in data.get('models', [])]
            return self.model in model_names
            
        except Exception as e:
            logger.error(f"Error checking model availability: {str(e)}")
            return False
            
    def get_config(self) -> Dict[str, Any]:
        """Get current Ollama configuration."""
        return self.config
        
    def generate(self, prompt: str, max_tokens: Optional[int] = None) -> Optional[str]:
        """Generate text using Ollama."""
        try:
            data = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": self.config["parameters"]
            }
            
            if max_tokens:
                data["options"]["num_predict"] = max_tokens
                
            response = httpx.post(
                f"{self.base_url}/api/generate",
                json=data,
                timeout=30.0
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("response")
            else:
                logger.error(f"Error generating text: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error calling Ollama API: {str(e)}")
            return None
            
    def list_models(self) -> list:
        """List available models."""
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=5.0)
            if response.status_code == 200:
                data = response.json()
                return [model['name'] for model in data.get('models', [])]
            return []
        except Exception as e:
            logger.error(f"Error listing models: {str(e)}")
            return []

    def pull_model(self):
        """Pull the required model from the Ollama registry."""
        from rich.console import Console
        from rich.progress import Progress

        console = Console()
        console.print(f"[bold green]Downloading model '{self.model}'... This may take a while.[/bold green]")

        try:
            with Progress(console=console) as progress:
                task = progress.add_task("[cyan]Downloading...[/]", total=None)
                
                with httpx.stream("POST", f"{self.base_url}/api/pull", json={"name": self.model}, timeout=None) as response:
                    if response.status_code != 200:
                        console.print(f"[red]Error: Failed to pull model. Status: {response.status_code}[/red]")
                        return

                    total = 0
                    completed = 0

                    for line in response.iter_lines():
                        if line:
                            data = json.loads(line)
                            if 'total' in data and 'completed' in data:
                                if total == 0:
                                    total = data['total']
                                    progress.update(task, total=total)
                                completed = data['completed']
                                progress.update(task, completed=completed, description=f"[cyan]Downloading... {data['status']}[/cyan]")
                            else:
                                progress.update(task, description=f"[cyan]{data.get('status', 'Working...')}[/cyan]")

            console.print(f"[green]Model '{self.model}' downloaded successfully.[/green]")

        except Exception as e:
            console.print(f"[red]An error occurred while pulling the model: {e}[/red]")