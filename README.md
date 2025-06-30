# Amazon Scraper

This project collects data from Amazon Seller Central and submits results to a Google Form. Sensitive configuration such as login credentials is loaded from `config.json` which is **not** tracked in version control.

## Setup

1. Copy `config.example.json` to `config.json`:

   ```bash
   cp config.example.json config.json
   ```

2. Edit `config.json` and provide your own credentials and URLs.

The GitHub Actions workflow builds a `config.json` from repository secrets. You can supply the same values locally via environment variables or by editing the file directly.

Run the scraper with:

```bash
python scraper.py
```
