# Gamecock - Financial Analysis & Swaps Intelligence Tool

A powerful, command-line tool for downloading SEC filings and analyzing financial swaps data. It leverages a local AI model (via Ollama) to provide deep insights, risk analysis, and plain-language explanations of complex financial instruments.

## Core Features

-   **SEC Filing Downloader**: Fetches company filings (10-K, 8-K, etc.) directly from the SEC EDGAR system.
-   **Automated Swaps Processing**: Automatically identifies and processes swaps data from downloaded files (`.csv`, `.json`), normalizing the data into a structured format.
-   **Normalized SQLite Database**: Stores all data in a local SQLite database, tracking the relationships between swaps, counterparties, reference securities, and their obligations.
-   **Quantitative Risk Engine**: Calculates detailed exposure metrics and a quantitative risk score (0-100) for any reference entity based on notional value, maturity, and counterparty/currency concentration.
-   **AI-Powered Analysis (Ollama)**:
    -   Generates plain-language explanations for complex swap contracts.
    -   Provides AI-driven executive summaries for risk reports.
-   **Interactive Data Explorer**: A rich, menu-driven interface to browse counterparties and securities, and drill down to view associated swaps and get AI explanations.

## System Requirements

-   Python 3.9+
-   [Ollama](https://ollama.ai/) installed and running locally.
-   An Ollama model downloaded (e.g., `ollama pull mistral`).
-   Minimum 8GB RAM (16GB+ recommended for large datasets).

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/gamecock.git
    cd gamecock
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment:**
    -   Rename `.env_template` to `.env`.
    -   Open the `.env` file and enter your email address as the `SEC_USER_AGENT`. This is required by the SEC for API access.
        ```
        SEC_USER_AGENT=your.email@example.com
        ```

## Getting Started

To launch the application, run the interactive menu from the root directory:

```bash
python -m gamecock.cli --menu
```

## Menu Walkthrough

The application provides a simple, interactive menu to access its features:

1.  **Search for Company**: Find a company by name or ticker to add it to your local database.

2.  **Download Filings**: Select a saved company to download its recent filings. The application will automatically process any swaps data found in the downloaded files.

3.  **Swaps Analysis**: Access the quantitative analysis tools.
    -   **Analyze Reference Entity Exposure**: Get a quick quantitative breakdown of exposure for a given security (e.g., 'GME').
    -   **Generate Risk Report**: Generate a detailed report for an entity, including a calculated risk score and an AI-generated executive summary.

4.  **Data Explorer**: Interactively browse the swaps database.
    -   **List All Counterparties / Securities**: View all unique counterparties and securities in the database.
    -   **Drill Down**: From the lists, you can select an entity by its ID to view all associated swaps.
    -   **Explain Swap**: From a list of swaps, you can select a contract by its ID to get a detailed, AI-generated explanation of its terms and risks.

## Development

### Running Tests

To run the test suite:

```bash
pytest
```

## License

This project is licensed under the MIT License.
