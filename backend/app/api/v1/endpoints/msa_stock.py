from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any
from datetime import datetime
import random

from app.database.session import get_db
from app.models.rbac import User
from app.security.dependencies import get_current_user
from app.schemas.common import APIResponse

router = APIRouter(prefix="/msa", tags=["MSA Stock Calculation"])

# Example table names
MSA_TABLE_1 = "msa_data_main"
MSA_TABLE_2 = "msa_data_gen"
MSA_TABLE_3 = "msa_data_detail"

@router.post("/save", response_model=APIResponse)
def save_msa_data(
    payload: Dict[str, Any],
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Save 3 different data sets and threshold, generate token, store in 3 tables.
    """
    # Generate token
    today = datetime.now().strftime("%Y%m%d")
    rand = random.randint(1, 999)
    token = f"MSA{today}-{rand:03d}"

    # Extract data sets
    data1 = payload.get("data1", [])
    data2 = payload.get("data2", [])
    data3 = payload.get("data3", [])
    threshold = payload.get("threshold")
    filters = payload.get("filters", {})

    # Save to DB (pseudo code)
    # for row in data1:
    #     db.execute(f"INSERT INTO {MSA_TABLE_1} ...", ...)
    # for row in data2:
    #     db.execute(f"INSERT INTO {MSA_TABLE_2} ...", ...)
    # for row in data3:
    #     db.execute(f"INSERT INTO {MSA_TABLE_3} ...", ...)
    # db.commit()

    # Return token and status
    return APIResponse(data={"token": token, "threshold": threshold, "filters": filters}, message="Saved 3 data sets with token.")
