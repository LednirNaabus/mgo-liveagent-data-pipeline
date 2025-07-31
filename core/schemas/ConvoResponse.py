from pydantic import BaseModel

class ResponseSchema(BaseModel):
    service_category: str  
    summary : str
    intent_rating : str
    engagement_rating : int
    clarity_rating : int
    resolution_rating : int
    sentiment_rating: str
    location: str
    schedule_date: str
    schedule_time: str
    car: str
    contact_num: str
    payment: str
    inspection: str
    quotation: str
    model: str