from pydantic import BaseModel, Field, field_validator
from typing import Literal

class ProposedTrade(BaseModel):
    """
    Data Contract для будь-якої ринкової заявки.
    Використовує strict=True для запобігання неявному приведенню типів.
    """
    action: Literal["BUY", "SELL", "HOLD"]
    volume_mw: float = Field(gt=0, le=100.0)  # Обмеження потужності
    target_price_uah: float = Field(ge=10.0, le=16000.0)  # Цінові капи України [38, 39]
    current_soc: float = Field(ge=0.0, le=1.0)
    
    @field_validator('action')
    @classmethod
    def check_physical_limits(cls, v: str, info):
        # Логіка Gatekeeper: якщо розряджаємо батарею при низькому заряді
        if v == "SELL" and info.data.get('current_soc', 1.0) < 0.05:
            raise ValueError("Блокування: Спроба розряду при SOC < 5%!") # [10]
        return v

    model_config = {"strict": True}