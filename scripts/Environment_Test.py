import sys
print(f"Using Python: {sys.executable}")

try:
    import kaggle
    print("Kaggle is installed and working!")
except ModuleNotFoundError:
    print("Kaggle module is missing!")
