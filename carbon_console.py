from carbonai_mistral import CarbonAIMistral
import sys
import time
import os

def print_slowly(text, delay=0.02):
    """Print text character by character for a cool effect"""
    for char in text:
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(delay)
    print()

def display_welcome():
    welcome_text = """
╔═══════════════════════════════════════════╗
║             Welcome to Carbon             ║
║    Your Expert on All Things Carbon!      ║
╚═══════════════════════════════════════════╝
    """
    print_slowly(welcome_text, 0.001)

def main():
    # Get Hugging Face token
    token = os.getenv('HF_TOKEN')
    if token is None:
        print("Please enter your Hugging Face token (get it from https://huggingface.co/settings/tokens):")
        token = input().strip()
        if not token:
            print("Error: Token is required to use Mistral-7B")
            return

    # Initialize the model
    print("Initializing Carbon AI...")
    try:
        carbon_ai = CarbonAIMistral(token=token)
        display_welcome()
        
        print_slowly("Hello! I'm Carbon, your expert on carbon-related topics. What would you like to know?")
        
        while True:
            print("\n" + "─" * 50)
            user_input = input("\nYou: ").strip()
            
            if user_input.lower() in ['exit', 'quit', 'bye']:
                print_slowly("\nCarbon: Goodbye! Thank you for chatting with me!")
                break
                
            if not user_input:
                continue
                
            print("\nCarbon: ", end="")
            try:
                response = carbon_ai.generate_response(user_input)
                print_slowly(response)
            except Exception as e:
                print_slowly(f"I apologize, but I encountered an error: {str(e)}")
                print_slowly("Please try asking your question in a different way.")
    except Exception as e:
        print(f"\nAn error occurred while initializing: {str(e)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_slowly("\n\nCarbon: Goodbye! Have a great day!")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
