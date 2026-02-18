"""
Events Handler Service - Ingest And Read Events

Routes:
    POST /events:                       Validate, Store, enqueue SQS
    GET /devices/{device_id}/events:    Query events

"""

import logging
import os
from typing import Any
import boto3
from botocore.endpoint import uuid
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError, field_validator
from decimal import Decimal
from time import time
import json
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


_dyndb = boto3.resource("dynamodb")
_sqs = boto3.resource("sqs")

EVENTS_TABLE = os.environ["EVENTS_TABLE"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

_queue = _sqs.Queue(SQS_QUEUE_URL)

_table = _dyndb.Table(EVENTS_TABLE)


class EventPayload(BaseModel):
    """
    Incoming Events Schema
    """

    device_id: str = Field(..., min_length=1, max_length=128)
    type: str = Field(..., min_length=1, max_length=64)
    value: float
    ts: int = Field(..., description="Epoch milliseconds")
    raw: dict | None = None

    # non empty str fields
    @field_validator("device_id", "type")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("device_id cannot be empty")
        return v

    # Ensure ts is positive
    @field_validator("ts")
    @classmethod
    def ts_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("ts must be a positive epoch millisecond value")
        return v


def _response(status: int, body: Any, request_id: str | None = None) -> dict:
    """
    Construct a structured output
    """
    payload = body if isinstance(body, dict) else {"message": body}
    if request_id:
        payload["request_id"] = request_id

    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "X-Request-Id": request_id or "",
        },
        "body": json.dumps(payload, default=str),
    }


def _parse_body(event: dict) -> EventPayload:
    """
    Convert the body from string to EventPayload Object
    """
    raw = event.get("body", None)
    if isinstance(raw, str):
        data = json.loads(raw)
        return EventPayload(**data)
    else:
        raise ValueError("Message Body is required")


def ingest_event(event: dict, context: Any) -> dict:
    """
    HTTP POST /events
    Body: {
            "device_id": STRING | REQUIRED,
            "type": STRING | REQUIRED,
            "value": INT | REQUIRED,
            "ts": INT | REQUIRED,
            "raw": ANY | OPTIONAL
           }

    RULES: device_id+ts should be unique
    """

    request_id = context.aws_request_id if context else str(uuid.uuid4())
    logger.info("ingest_event called", extra={"request_id": request_id})

    # Processing & Validate input
    try:
        payload = _parse_body(event)

    # Raise Error for json errors
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON body", extra={"error": str(e)})
        return _response(
            400, {"error": "Invalid JSON body", "detail": str(e)}, request_id
        )

    # Raise error: device_id == None or ts < 0
    except ValidationError as e:
        logger.warning("Validation failed", extra={"errors": e.errors()})
        return _response(
            400, {"error": "Validation failed", "detail": e.errors()}, request_id
        )

    # Construct item for DynamoDB
    pk = f"DEVICE#{payload.device_id}"
    sk = f"TS#{payload.ts}"

    item = {
        "PK": pk,
        "SK": sk,
        "device_id": payload.device_id,
        "type": payload.type,
        "value": Decimal(str(payload.value)),
        "ts": payload.ts,
        "ingested_at": Decimal(str(time() * 1000)),
        "request_id": request_id,
    }
    if payload.raw:
        item["raw"] = payload.raw

    try:
        _table.put_item(
            Item=item,
            # Unique device_id + ts
            ConditionExpression="attribute_not_exists(SK)",
        )

        logger.info("Event stored", extra={"pk": pk, "sk": sk})

    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        # Dublicate entry
        if code == "ConditionalCheckFailedException":
            logger.info("Duplicate event ignored", extra={"pk": pk, "sk": sk})
            return _response(
                409,
                {
                    "message": "Duplicate event — already stored",
                    "request_id": request_id,
                },
            )
        logger.error("DynamoDB write failed", extra={"error": str(exc)})
        # Error while adding to DB
        return _response(500, {"error": "Failed to store event"}, request_id)

    # DB insertion success, Add event to SQS
    sqs_msg = {
        "device_id": payload.device_id,
        "type": payload.type,
        "value": payload.value,
        "ts": payload.ts,
        "request_id": request_id,
        "event_id": f"{pk}:{sk}",
    }

    try:
        resp = _queue.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(sqs_msg),
        )
        logger.info("Event enqueued", extra={"message_id": resp["MessageId"]})

    except ClientError as exc:
        # Enqueue failed, reasons: Network error, SQS does not exists, Permission errors
        logger.error("SQS enqueue failed", extra={"error": str(exc)})
        return _response(500, {"message": "SQS Enqueue failed,", "error": str(exc)})

    # Success in adding to DB, SQS insertion async
    return _response(
        201,
        {
            "message": "Event ingested",
            "device_id": payload.device_id,
            "ts": payload.ts,
            "request_id": request_id,
        },
    )


def get_device_events(event: dict, context: Any) -> dict:
    """
    HTTP GET /devices/{device_id}/events
    Parameters:
                - from_ts : +int | OPTIONAL
                - to_ts   : +int | OPTIONAL
                - limit   : +int | OPTIONAL default = 100
    """
    request_id = context.aws_request_id if context else str(uuid.uuid4())
    logger.info("get_device_events called", extra={"request_id": request_id})

    # Request validation
    path_params = event.get("pathParameters") or {}
    device_id = (path_params.get("device_id") or "").strip()

    if not device_id:
        return _response(
            400, {"error": "device_id path parameter is required"}, request_id
        )

    query_params = event.get("queryStringParameters") or {}
    from_ts = query_params.get("from_ts")
    to_ts = query_params.get("to_ts")
    limit = int(query_params.get("limit", 100))

    pk = f"DEVICE#{device_id}"

    # Construct Query
    key_condition = Key("PK").eq(pk)
    if from_ts and to_ts:
        key_condition &= Key("SK").between(f"TS#{from_ts}", f"TS#{to_ts}")
    elif from_ts:
        key_condition &= Key("SK").gte(f"TS#{from_ts}")
    elif to_ts:
        key_condition &= Key("SK").lte(f"TS#{to_ts}")

    try:
        result = _table.query(
            KeyConditionExpression=key_condition,
            Limit=limit,
            ScanIndexForward=False,  # reverse the ts ie newest first
        )

    except ClientError as exc:
        logger.error("DynamoDB query failed", extra={"error": str(exc)})
        return _response(500, {"error": "Failed to retrieve events"}, request_id)

    # Convert value, ts, ingested_at for JSON
    items = []
    for row in result.get("Items", []):
        row["value"] = float(row["value"])
        row["ts"] = int(row["ts"])
        row["ingested_at"] = int(row["ingested_at"])
        items.append(row)

    logger.info(
        "Events retrieved",
        extra={"device_id": device_id, "count": len(items)},
    )

    return _response(
        200,
        {
            "device_id": device_id,
            "count": len(items),
            "events": items,
            "last_evaluated_key": result.get("LastEvaluatedKey"),
        },
        request_id,
    )
