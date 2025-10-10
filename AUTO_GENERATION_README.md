# AI-Based Field Tag Auto-Generation

## Overview
This feature automatically generates `field_tag_mappings.json` using AI/LLM analysis of your database schema. The AI uses your `data_masking_config.json` tag definitions as a reference guide to classify fields appropriately.

## How It Works

1. **Input Sources:**
   - `data/catalog_live.json` - Your database schema
   - `data_masking_config.json` - Available tag definitions and descriptions

2. **AI Analysis:**
   - Sends schema + tag definitions to LLM
   - AI analyzes field names, data types, and table context
   - Suggests appropriate tags for each field

3. **Output:**
   - Generates `field_tag_mappings.json` with classifications
   - Validates that only defined tags are used

## Setup

### Method 1: Using .env file (Recommended)
1. **Create `.env` file in project root:**
   ```bash
   OPENAI_API_KEY=your_openai_api_key_here
   OPENAI_MODEL=gpt-4o
   OPENAI_BASE_URL=https://api.openai.com/v1/chat/completions
   ```

2. **Copy from example:**
   ```bash
   copy .env.example .env
   # Then edit .env with your actual API key
   ```

### Method 2: Environment Variables
```bash
set OPENAI_API_KEY=your_openai_api_key_here
set OPENAI_MODEL=gpt-4o
set OPENAI_BASE_URL=https://api.openai.com/v1/chat/completions
```

## Usage

### Method 1: Via Streamlit UI
1. Run `streamlit run app/ui_streamlit.py`
2. Click "Auto-Generate Field Tags" in the sidebar
3. Wait for AI analysis to complete
4. Review generated mappings

### Method 2: Via Command Line
```bash
python test_auto_generation.py
```

### Method 3: Programmatically
```python
from app.auto_tag_generator import auto_generate_field_tag_mappings

mappings = auto_generate_field_tag_mappings(
    catalog_file="data/catalog_live.json",
    config_file="data_masking_config.json",
    output_file="field_tag_mappings.json",
    force_regenerate=True
)
```

## Tag Classification Logic

The AI considers:
- **Field names:** `billing_email` → `pii`, `contact_info`
- **Data types:** `numeric` amounts → `financial`
- **Table context:** Fields in `payments` table → `financial`
- **Business patterns:** `*_id` fields → `identifier`

## Benefits

✅ **Fully Automated** - No manual field mapping needed  
✅ **Consistent** - Uses your defined tag vocabulary  
✅ **Scalable** - Works with any database size  
✅ **Smart** - Understands business context and patterns  
✅ **Customizable** - Guided by your tag definitions  

## Example Output

```json
{
  "table_mappings": {
    "Billing_core.public.invoices": {
      "table_tags": ["financial", "billing"],
      "column_tags": {
        "total_amount": ["financial"],
        "billing_email": ["pii", "contact_info"],
        "client_code": ["business_key"]
      }
    }
  }
}
```

## Troubleshooting

- **"OPENAI_API_KEY not set"** → Set the environment variable
- **"Import could not be resolved"** → Ensure auto_tag_generator.py is in app/ directory
- **"Invalid JSON"** → LLM response parsing failed, try again
- **"Undefined tags"** → AI used tags not in your config, check tag_definitions

## Cost Considerations

- Typical cost: $0.01-0.10 per database depending on schema size
- Uses gpt-4o model by default (efficient and accurate)
- One-time generation - results are cached in field_tag_mappings.json