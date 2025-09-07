import json
import subprocess
import sys
import os
import platform
import requests
import psutil
import GPUtil
import time
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / 'system_settings.json'

def load_settings(config_path=None):
    """Load system settings from config file."""
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
        
    with open(config_path, 'r') as f:
        return json.load(f)

def save_settings(settings, config_path=None):
    """Save system settings to config file."""
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
        
    config_path = Path(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_path, 'w') as f:
        json.dump(settings, f, indent=4)

def get_system_specs():
    """Get system specifications."""
    specs = {
        'os': platform.system(),
        'cpu_cores': psutil.cpu_count(logical=False),
        'cpu_threads': psutil.cpu_count(logical=True),
        'ram_gb': psutil.virtual_memory().total / (1024**3),
        'gpu_info': None
    }
    
    try:
        gpus = GPUtil.getGPUs()
        if gpus:
            gpu = gpus[0]  # Get primary GPU
            specs['gpu_info'] = {
                'name': gpu.name,
                'vram_gb': gpu.memoryTotal / 1024,  # Convert MB to GB
                'cuda_version': None  # Will be filled if CUDA is available
            }
            
            # Check CUDA version
            try:
                result = subprocess.run(['nvidia-smi', '--query-gpu=driver_version', '--format=csv,noheader'], 
                                     capture_output=True, text=True)
                if result.returncode == 0:
                    specs['gpu_info']['cuda_version'] = result.stdout.strip()
            except:
                pass
    except:
        pass
        
    return specs

def is_ollama_running(settings):
    """Check if Ollama is running."""
    try:
        response = requests.get(f"{settings['ollama']['api_host']}/api/tags")
        return response.status_code == 200
    except:
        return False

def is_mistral_available(settings):
    """Check if Mistral model is available in Ollama."""
    try:
        response = requests.get(f"{settings['ollama']['api_host']}/api/tags")
        if response.status_code == 200:
            models = response.json().get('models', [])
            return any(model.get('name', '').startswith(settings['ollama']['model_name']) for model in models)
        return False
    except:
        return False

def install_ollama(settings):
    """Install Ollama if not present."""
    if not settings['system']['auto_install']:
        print("Auto-install is disabled in settings. Please install Ollama manually.")
        return False
        
    system = platform.system().lower()
    
    if system == 'windows':
        # Download Windows installer
        print("Downloading Ollama installer...")
        subprocess.run([
            'powershell', 
            '-Command',
            'Invoke-WebRequest -Uri https://ollama.ai/download/ollama-windows-amd64.msi -OutFile ollama-installer.msi'
        ], check=True)
        
        # Install Ollama
        print("Installing Ollama...")
        subprocess.run(['msiexec', '/i', 'ollama-installer.msi', '/quiet'], check=True)
        
        # Clean up
        os.remove('ollama-installer.msi')
        
        # Wait for service to start
        time.sleep(10)
    else:
        print("Unsupported operating system")
        return False
        
    return True

def optimize_ollama_config(settings, specs=None):
    """Create optimized Ollama configuration based on settings and system specs."""
    if not settings['ollama']['auto_detect']:
        return {
            'name': settings['ollama']['model_name'],
            'parameters': settings['ollama']['manual_settings']
        }
    
    if specs is None:
        specs = get_system_specs()
    
    # Update system info in settings
    settings['system'].update({
        'ram_gb': specs['ram_gb'],
        'cpu_cores': specs['cpu_cores'],
        'gpu_vram_gb': specs['gpu_info']['vram_gb'] if specs['gpu_info'] else None,
        'cuda_available': specs['gpu_info']['cuda_version'] if specs['gpu_info'] else None
    })
    
    # Create optimized config
    config = {
        'name': settings['ollama']['model_name'],
        'parameters': {
            'num_ctx': min(32768, int(specs['ram_gb'] * 128)),
            'num_gpu': 1 if specs['gpu_info'] else 0,
            'num_thread': specs['cpu_cores'],
            'batch_size': 512 if specs['gpu_info'] and specs['gpu_info']['vram_gb'] >= 8 else 256,
            'temperature': settings['ollama']['manual_settings']['temperature']
        }
    }
    
    # Adjust for GPU memory
    if specs['gpu_info']:
        vram_gb = specs['gpu_info']['vram_gb']
        if vram_gb >= 8:
            config['parameters']['num_gpu_layers'] = -1
        else:
            config['parameters']['num_gpu_layers'] = int(vram_gb * 8)
            
    return config

def setup_ollama_model(config_path=None):
    """Setup and configure Ollama with model."""
    try:
        # Load settings
        settings = load_settings(config_path)
        print("Loaded settings from config file")
        
        if settings['ollama']['auto_detect']:
            print("\nChecking system configuration...")
            specs = get_system_specs()
            print(f"System specs: {json.dumps(specs, indent=2)}")
        else:
            print("\nUsing manual settings from config file")
            specs = None
        
        # Check if Ollama is running
        if not is_ollama_running(settings):
            print("\nOllama is not running. Checking if installation is needed...")
            try:
                subprocess.run(['ollama', 'version'], capture_output=True)
                print("Ollama is installed but not running. Please start Ollama service.")
                return False
            except FileNotFoundError:
                if settings['system']['auto_install']:
                    print("Ollama is not installed. Installing...")
                    if not install_ollama(settings):
                        print("Failed to install Ollama")
                        return False
                else:
                    print("Ollama is not installed and auto-install is disabled")
                    return False
        
        print("\nOllama is running")
        
        # Check if model is available
        if not is_mistral_available(settings):
            print(f"\nDownloading {settings['ollama']['model_name']} model...")
            subprocess.run(['ollama', 'pull', settings['ollama']['model_name']], check=True)
        
        # Create optimized configuration
        config = optimize_ollama_config(settings, specs)
        print(f"\nGenerated configuration: {json.dumps(config, indent=2)}")
        
        # Save updated settings
        save_settings(settings, config_path)
        print("\nUpdated settings saved to config file")
        
        print("\nSystem is ready to use Ollama with configured settings")
        return True
        
    except Exception as e:
        print(f"Error during setup: {str(e)}")
        return False

if __name__ == '__main__':
    if setup_ollama_model():
        print("Setup completed successfully")
    else:
        print("Setup failed")
