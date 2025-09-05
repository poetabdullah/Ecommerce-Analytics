"""
Defines Pydantic models for:
 - Raw customer records (direct from API)
 - Processed customer records (after transformation)
Provides validation and default handling.
"""

from typing import Optional, Literal
from pydantic import BaseModel, Field, EmailStr, validator


class RawCustomer(BaseModel):
    # Schema for customer records as returned by the Reqres API.
    id: int
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    avatar: Optional[str] = None


class ProcessedCustomer(BaseModel):
    # Schema for standardized customer records for analytics.
    customer_id: int
    full_name: str
    email_domain: str = Field(default="unknown")

    # Business enrichment fields
    engagement_level: Literal["high", "medium", "low", "unknown"]
    activity_status: Literal["active", "inactive", "unknown"]
    acquisition_channel: Literal["website", "mobile_app", "email_campaign", "unknown"]
    market_segment: Literal["US-West", "US-East", "EU-Central", "APAC", "unknown"]
    customer_tier: Literal["basic", "premium", "enterprise", "unknown"]

    data_quality_score: int = Field(ge=0, le=100)

    @validator("full_name")
    def validate_full_name(cls, v: str) -> str:
        if not v or not v.strip():
            return "Unknown"
        return v.strip()

    @validator("email_domain", pre=True, always=True)
    def normalize_email_domain(cls, v: str) -> str:
        if not v or not v.strip():
            return "unknown"
        return v.lower().strip()
