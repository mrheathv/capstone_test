# Getting Started - Development Setup
This guide will help you set up the development environment to work on the rag_salesbot project.
## Prerequisites
- **Python 3.8+** - [Download here](https://www.python.org/downloads/)
- **Git** - For cloning and version control
- **Text Editor or IDE** - VS Code, PyCharm, etc.

### 1. Create a Virtual Environment
A virtual environment isolates project dependencies from your system Python.
**On macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```
**On Windows:**
```bash
python -m venv .venv
.venv\Scripts\activate
```
You should see `(.venv)` appear in your terminal prompt when activated.
### 2. Install Requirements
Once the virtual environment is activated, install all project dependencies:
```bash
pip install -r requirements.txt
```
### 3. Set Up Your API Keys
The application uses OpenAI's API. You'll need to:
1. Get an API key from [OpenAI](https://platform.openai.com/account/api-keys)
2. Create a `.env` file in the project root:
  ```bash
  echo "OPENAI_API_KEY=your_key_here" > .env
  ```
3. Or set the environment variable:
  ```bash
  export OPENAI_API_KEY=your_key_here
  ```
## Launching the App
Once setup is complete and your virtual environment is activated:
```bash
streamlit run app/text_to_sql_app.py
```
The app will open in your browser at `http://localhost:8501`
## Project Structure
```
app/                 # Main application code
├── text_to_sql_app.py    # Streamlit entry point
├── agent/            # AI agent logic
│   ├── core.py       # Core agent functionality
│   ├── text_to_sql.py    # SQL generation tools
│   ├── tools.py      # Tool definitions
│   └── open_work.py  # Additional handlers
└── database/         # Database interaction
   ├── connection.py # DuckDB connection
   └── schema.py     # Database schema
data/                # Sample CSV data files
db/                  # DuckDB database files
loaders/             # Data loading scripts
sql/                 # SQL scripts
prompts/             # Prompt templates
```
## Common Tasks
### Deactivating the Virtual Environment
When you're done working:
```bash
deactivate
```
### Reinstalling Dependencies
If dependencies change or become corrupted:
```bash
pip install -r requirements.txt --force-reinstall
```
### Checking Installed Packages
```bash
pip list
```
## Troubleshooting
**"command not found: python3"**
- Ensure Python is installed and added to your PATH
- Try using `python` instead of `python3`
**"ModuleNotFoundError" when running the app**
- Make sure your virtual environment is activated (`(.venv)` should show in prompt)
- Reinstall requirements: `pip install -r requirements.txt`
**"OPENAI_API_KEY not set"**
- Verify your `.env` file exists and contains the key
- Or set it as an environment variable (see Setup step 4)