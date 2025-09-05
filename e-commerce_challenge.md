# Challenge: E-commerce Analytics Data Pipeline

## Problem Overview

You're building a **data synchronization service** for an e-commerce analytics platform that processes customer engagement data from multiple sources. The service needs to fetch, clean, and standardize customer data from a **simulated third-party API** that mimics real-world challenges like pagination, network failures, and data inconsistencies.

This challenge tests your ability to build **production-ready Python services** with proper error handling, retry logic, data transformation, and clean architecture.

---

## Business Context

**E-commerce Analytics Co.** needs to analyze customer engagement patterns to optimize their marketing campaigns. They receive customer data from various touchpoints (website, mobile app, email campaigns) through a third-party API that has some reliability issues.

Your task is to build a robust data pipeline that:
- Fetches all customer data reliably despite API failures
- Cleans and standardizes the data for analytics
- Handles edge cases and data quality issues
- Provides clear logging and error reporting

---

## Sample Dataset & API Behavior

### Real API Endpoint: `GET /api/users`

**Base URL:** `https://reqres.in/api`

**Note:** This is a real, publicly available API that you can test immediately!

### Real API Response Structure:
```json
{
  "page": 1,
  "per_page": 6,
  "total": 12,
  "total_pages": 2,
  "data": [
    {
      "id": 1,
      "email": "george.bluth@reqres.in",
      "first_name": "George",
      "last_name": "Bluth",
      "avatar": "https://reqres.in/img/faces/1-image.jpg"
    },
    {
      "id": 2,
      "email": "janet.weaver@reqres.in",
      "first_name": "Janet", 
      "last_name": "Weaver",
      "avatar": "https://reqres.in/img/faces/2-image.jpg"
    },
    {
      "id": 3,
      "email": "emma.wong@reqres.in",
      "first_name": "Emma",
      "last_name": "Wong", 
      "avatar": "https://reqres.in/img/faces/3-image.jpg"
    }
  ]
}
```

**Test the API yourself:**
- Page 1: `https://reqres.in/api/users?page=1`
- Page 2: `https://reqres.in/api/users?page=2`

### API Challenges (Real-world Simulation):

1. **Pagination**: Data is split across 2 pages (6 users per page, 12 total)
2. **Network Instability**: Simulate 20% of requests failing with HTTP 500/503 errors
3. **Rate Limiting**: Simulate 429 (Too Many Requests) if called too frequently  
4. **Data Transformation**: Convert API data to your business format
5. **Data Enrichment**: Add business logic fields (engagement_level, activity_status, etc.)

---

## Technical Requirements

### 1. API Client Class (`CustomerAPIClient`)

Create a robust API client that handles:

```python
class CustomerAPIClient:
    def __init__(self, base_url: str, api_key: str):
        # Initialize with base URL and authentication
        
    def fetch_all_customers(self) -> List[Dict]:
        # Fetch all pages with pagination
        # Handle retries with exponential backoff
        # Deduplicate customers by ID
        # Return clean list of all customers
        
    def _fetch_page(self, page: int) -> Dict:
        # Fetch single page with retry logic
        # Handle rate limiting (429 responses)
        # Log all failures and retries
```

**Retry Strategy:**
- Max 3 retries for failed requests
- Exponential backoff: 1s, 2s, 4s delays
- Special handling for 429 (rate limit) vs 500 (server error)
- Log all retry attempts with timestamps

### 2. Data Processor Class (`CustomerDataProcessor`)

Transform raw API data into analytics-ready format:

```python
class CustomerDataProcessor:
    def process_customers(self, raw_customers: List[Dict]) -> List[Dict]:
        # Transform data according to business rules
        # Handle data quality issues
        # Return standardized customer records
```

**Transformation Rules:**
```python
# Input format (from Reqres API)
{
    "id": 1,
    "email": "george.bluth@reqres.in",
    "first_name": "George",
    "last_name": "Bluth",
    "avatar": "https://reqres.in/img/faces/1-image.jpg"
}

# Output format (for analytics) - You need to add business logic
{
    "customer_id": 1,
    "full_name": "George Bluth",
    "email_domain": "reqres.in", 
    "engagement_level": "high",        # Generate randomly: high/medium/low
    "activity_status": "active",       # Generate randomly: active/inactive
    "acquisition_channel": "website",  # Generate randomly: website/mobile/email
    "market_segment": "US-West",       # Generate randomly: US-West/US-East/EU/APAC
    "customer_tier": "premium",        # Generate randomly: basic/premium/enterprise
    "data_quality_score": 100          # 100 = perfect, deduct for missing fields
}
```

**Business Logic (You need to implement):**
- `engagement_level`: Generate randomly (high/medium/low) or use a scoring algorithm
- `activity_status`: Generate randomly (active/inactive) or use date logic
- `email_domain`: Extract domain from email (handle malformed emails gracefully)
- `acquisition_channel`: Generate randomly (website/mobile_app/email_campaign)
- `market_segment`: Generate randomly (US-West/US-East/EU-Central/APAC)
- `customer_tier`: Generate randomly (basic/premium/enterprise)
- `data_quality_score`: 100 - (10 points per missing field)

### 3. Data Quality Handling

Handle these edge cases:
- **Missing emails**: Set `email_domain` to "unknown", reduce quality score
- **Invalid dates**: Set `activity_status` to "unknown", reduce quality score  
- **Null engagement scores**: Set `engagement_level` to "unknown", reduce quality score
- **Duplicate customers**: Keep the record with highest `data_quality_score`

### 4. Export & Reporting

```python
class DataExporter:
    def export_customers(self, customers: List[Dict], output_file: str):
        # Save to JSON file
        # Sort by full_name (ascending)
        # Include metadata (total count, quality stats)
        
    def generate_summary_report(self, customers: List[Dict]) -> Dict:
        # Return summary statistics
```

**Output Format:**
```json
{
  "metadata": {
    "total_customers": 12,
    "export_timestamp": "2024-01-20T15:30:00Z",
    "data_quality_summary": {
      "high_quality": 8,
      "medium_quality": 3, 
      "low_quality": 1
    }
  },
  "customers": [
    {
      "customer_id": "cust_001",
      "full_name": "Sarah Johnson",
      "email_domain": "techcorp.com",
      "engagement_level": "high",
      "activity_status": "active",
      "acquisition_channel": "website",
      "market_segment": "US-West", 
      "customer_tier": "premium",
      "data_quality_score": 100
    }
    // ... more customers sorted by name
  ]
}
```

---

## Testing Requirements

Write comprehensive tests using `pytest` or `unittest`:

```python
# Test cases to implement:
def test_api_client_retry_logic():
    # Test retry behavior on failures
    
def test_data_processor_transformation():
    # Test business logic transformations
    
def test_duplicate_handling():
    # Test deduplication logic
    
def test_data_quality_scoring():
    # Test quality score calculations
    
def test_edge_cases():
    # Test malformed data handling
```

---

## Success Criteria

Your solution should demonstrate:

 **Robust Error Handling**: Graceful handling of API failures, rate limits, and malformed data  
 **Clean Architecture**: Well-structured classes with single responsibilities  
 **Production Readiness**: Proper logging, retry logic, and error reporting  
 **Data Quality**: Comprehensive handling of edge cases and data inconsistencies  
 **Testability**: Modular code with good test coverage  
 **Documentation**: Clear docstrings and inline comments  

---

## Bonus Challenges

### Advanced Features (Optional):
1. **Async Implementation**: Use `asyncio` and `aiohttp` for concurrent API calls
2. **Caching**: Implement Redis/memory caching to avoid redundant API calls
3. **Monitoring**: Add metrics collection (success rates, processing times)
4. **Configuration**: Use environment variables or config files for API settings
5. **Data Validation**: Use Pydantic models for data validation

### Performance Optimization:
- Process data in chunks for large datasets
- Implement connection pooling for API calls
- Add progress tracking for long-running operations

---

## Expected Project Structure

```
customer_data_pipeline/
├── src/
│   ├── api_client.py          # CustomerAPIClient class
│   ├── data_processor.py      # CustomerDataProcessor class  
│   ├── exporter.py            # DataExporter class
│   ├── models.py              # Data models/Pydantic schemas
│   └── main.py                # Main orchestration script
├── tests/
│   ├── test_api_client.py
│   ├── test_data_processor.py
│   └── test_integration.py
├── requirements.txt
├── README.md
└── sample_output.json
```

---

## Evaluation Rubric

| Criteria | Weight | Description |
|----------|--------|-------------|
| **Code Quality** | 25% | Clean, readable, well-structured code |
| **Error Handling** | 25% | Robust retry logic and edge case handling |
| **Architecture** | 20% | Good separation of concerns, modular design |
| **Testing** | 15% | Comprehensive test coverage |
| **Documentation** | 10% | Clear docstrings and README |
| **Bonus Features** | 5% | Advanced features and optimizations |

---

## Getting Started

1. **Setup**: Create a new Python project with proper structure
2. **Mock API**: Use the provided sample data to simulate API responses
3. **Implementation**: Build the three core classes with proper error handling
4. **Testing**: Write tests for all major functionality
5. **Documentation**: Document your approach and any assumptions

**Time Estimate**: 3-4 hours for core implementation, 1-2 hours for testing and documentation

---

*This challenge simulates real-world data engineering scenarios you'd encounter in production systems. Focus on building maintainable, robust code that can handle the complexities of external APIs and data quality issues.*
