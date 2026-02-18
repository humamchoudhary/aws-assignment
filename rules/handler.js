/**
 * Rules + Alerts Service — Node.js Lambda Handler
 *
 * POST /rules           : Create an alert rule for a device
 * GET  /rules           : List rules (filter by ?device_id=)
 * GET  /alerts          : List triggered alerts (filter by ?device_id=)
 * SQS  evaluateAlerts   : Consume events, evaluate rules, store alerts
 *                         Supports partial batch failure (ReportBatchItemFailures)
 */

const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  ScanCommand,
} = require("@aws-sdk/lib-dynamodb");

const { randomUUID } = require("crypto");

const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

const sqsClient = new SQSClient({});

const RULES_TABLE = process.env.RULES_TABLE;
const ALERTS_TABLE = process.env.ALERTS_TABLE;

const DLQURL = process.env.EventsDLQUrl;
const OPERATORS = new Set([">", "<", ">=", "<=", "=="]);

/**
 * Return a semi-structured response
 */
const response = (statusCode, body, requestId = null) => ({
  statusCode,
  headers: {
    "Content-Type": "application/json",
    "X-Request-Id": requestId ?? "",
  },
  body: JSON.stringify({
    ...body,
    ...(requestId ? { request_id: requestId } : {}),
  }),
});

/**
 * Validate a rule creation payload.
 * Returns { valid: true, data } or { valid: false, errors: [...] }
 */
const validateRule = (body) => {
  const errors = [];

  if (!body || typeof body !== "object") {
    return { valid: false, errors: ["Request body must be a JSON object"] };
  }

  const { device_id, metric, operator, threshold, enabled } = body;

  if (!device_id || typeof device_id !== "string" || device_id.trim() === "") {
    errors.push("device_id is required and must be a non-empty string");
  }
  if (device_id && device_id.includes("#")) {
    errors.push('device_id must not contain "#"');
  }

  if (!metric || typeof metric !== "string" || metric.trim() === "") {
    errors.push("metric is required and must be a non-empty string");
  }

  if (!operator || !OPERATORS.has(operator)) {
    errors.push(
      `operator is required and must be one of: ${[...OPERATORS].join(", ")}`,
    );
  }

  if (
    threshold === undefined ||
    threshold === null ||
    typeof threshold !== "number"
  ) {
    errors.push("threshold is required and must be a number");
  }

  if (enabled !== undefined && typeof enabled !== "boolean") {
    errors.push("enabled must be a boolean");
  }

  if (errors.length) return { valid: false, errors };

  return {
    valid: true,
    data: {
      device_id: device_id.trim(),
      metric: metric.trim(),
      operator,
      threshold,
      enabled: enabled !== undefined ? enabled : true, // default to true if not provided
    },
  };
};

/**
 * Validate an incoming event.
 * Returns { valid: true, data } or { valid: false, errors: [...] }
 * Extra failsafe not needed as SQS is already properly formated
 */
const validateEvent = (body) => {
  const errors = [];
  if (!body || typeof body !== "object") {
    return { valid: false, errors: ["Event body must be a JSON object"] };
  }

  const { device_id, type, value, ts, request_id, event_id } = body;

  if (!device_id || typeof device_id !== "string") {
    errors.push("device_id is required and must be a string");
  }
  if (!type || typeof type !== "string") {
    errors.push("metricis required and must be a string");
  }
  if (value === undefined || typeof value !== "number") {
    errors.push("value is required and must be a number");
  }
  if (!ts || typeof ts !== "number") {
    errors.push("ts (timestamp) is required and must be a number");
  }
  if (!event_id || typeof event_id !== "string") {
    errors.push("event_id is required and must be a string");
  }

  if (errors.length) return { valid: false, errors };

  return {
    valid: true,
    data: { device_id, type, value, ts, request_id, event_id },
  };
};

const evaluate = (value, operator, threshold) => {
  switch (operator) {
    case ">":
      return value > threshold;
    case "<":
      return value < threshold;
    case ">=":
      return value >= threshold;
    case "<=":
      return value <= threshold;
    case "==":
      return value === threshold;
    default:
      return false;
  }
};

/**
 * Create a new rule.
 *
 * HTTP POST /rules
 *
 * Request body:
 * {
 *   "device_id": string,
 *   "metric": string,
 *   "operator": string,   // e.g. ">", "<", ">=", "<=", "=="
 *   "threshold": number,
 *   "enabled": boolean
 * }
 */

exports.createRule = async (event, context) => {
  const requestId = context?.awsRequestId ?? randomUUID();
  console.log("createRule called", { requestId });
  let body;

  // Request body validation
  try {
    body = JSON.parse(event.body);
  } catch (e) {
    console.error(e);
    console.error("Invalid body");
    return response(500, {
      message: "Invalid body",
      error: e,
    });
  }

  if (body === null) {
    return response(400, { error: "Invalid JSON body" }, requestId);
  }

  const validation = validateRule(body);
  if (!validation.valid) {
    console.log("Validation failed", { errors: validation.errors });
    return response(
      400,
      { error: "Validation failed", detail: validation.errors },
      requestId,
    );
  }

  // Insert Rule to dynamoDB
  const { device_id, metric, operator, threshold, enabled } = validation.data;
  const ruleId = randomUUID();
  const createdAt = Date.now();
  const pk = `RULE#${ruleId}`;

  const item = {
    PK: pk,
    rule_id: ruleId,
    device_id,
    metric,
    operator,
    threshold,
    enabled,
    created_at: createdAt,
  };

  try {
    await dynamo.send(new PutCommand({ TableName: RULES_TABLE, Item: item }));
    console.log("Rule created", { ruleId, device_id });
  } catch (err) {
    console.log("DynamoDB put failed", { error: err.message });
    return response(500, { error: `Failed to create rule: ${err}` }, requestId);
  }

  return response(
    201,
    { message: "Rule created", rule_id: ruleId, ...item },
    requestId,
  );
};

/**
 * Get rules, optionally filtered by a specific device.
 *
 * HTTP GET /rules
 *
 * Query parameters:
 * - device_id (string, optional): Filter rules for a specific device
 * - limit     (number, optional): Maximum number of rules to return (default: 100)
 */

exports.listRules = async (event, context) => {
  const requestId = context?.awsRequestId ?? randomUUID();
  console.log("listRules called", { requestId });

  const qs = event.queryStringParameters ?? {};
  const deviceId = qs.device_id?.trim();
  const limit = parseInt(qs.limit ?? "100", 10);

  let items;

  try {
    if (deviceId) {
      // Query Device only rules
      // Use the GSI to filter by device_id
      const result = await dynamo.send(
        new QueryCommand({
          TableName: RULES_TABLE,
          IndexName: "device_id-index",
          KeyConditionExpression: "device_id = :did",
          ExpressionAttributeValues: { ":did": deviceId },
          Limit: limit,
        }),
      );
      items = result.Items ?? [];
    } else {
      // Full scan
      const result = await dynamo.send(
        new ScanCommand({
          TableName: RULES_TABLE,
          Limit: limit,
        }),
      );
      items = result.Items ?? [];
    }
  } catch (err) {
    console.log("DynamoDB query/scan failed", { error: err.message });
    return response(500, { error: "Failed to retrieve rules" }, requestId);
  }

  console.log("Rules retrieved", {
    count: items.length,
    device_id: deviceId ?? "all",
  });
  return response(200, { count: items.length, rules: items }, requestId);
};

/**
 * Get alerts, optionally filtered by a specific device.
 *
 * HTTP GET /alerts
 *
 * Query parameters:
 * - device_id (string, optional): Filter alerts for a specific device
 * - limit     (number, optional): Maximum number of alerts to return (default: 100)
 */

exports.getAlerts = async (event, context) => {
  const requestId = context?.awsRequestId ?? randomUUID();
  console.log("getAlerts called", { requestId });

  const qs = event.queryStringParameters ?? {};
  const deviceId = qs.device_id?.trim();
  const limit = parseInt(qs.limit ?? "100", 10);

  let items;

  try {
    if (deviceId) {
      // Query Device only alerts, GSI or device_id
      const result = await dynamo.send(
        new QueryCommand({
          TableName: ALERTS_TABLE,
          IndexName: "device_id-index",
          KeyConditionExpression: "device_id = :did",
          ExpressionAttributeValues: { ":did": deviceId },
          Limit: limit,
          ScanIndexForward: false,
        }),
      );
      items = result.Items ?? [];
    } else {
      // List all
      const result = await dynamo.send(
        new ScanCommand({
          TableName: ALERTS_TABLE,
          Limit: limit,
        }),
      );
      items = result.Items ?? [];
    }
  } catch (err) {
    console.log("DynamoDB query/scan failed", { error: err.message });
    return response(500, { error: "Failed to retrieve alerts" }, requestId);
  }

  console.log("Alerts retrieved", { count: items.length });
  return response(200, { count: items.length, alerts: items }, requestId);
};

/**
 * Triggered by an SQS message.
 *
 * Message body schema:
 * {
 *   "device_id": string,
 *   "type": string,
 *   "value": number,
 *   "ts": number,          // epoch milliseconds
 *   "request_id": string,
 *   "event_id": string     // "<PK>:<SK>"
 * }
 */
exports.evaluateAlerts = async (event, context) => {
  const requestId = context?.awsRequestId ?? randomUUID();
  const { Records } = event;
  console.log(
    "evaluateAlerts Start",
    `Evaluation of alert started, ${requestId} : batchSize: ${Records.length}, records: ${Records}}`,
  );

  /** @type {{ itemIdentifier: string }[]} */
  const batchItemFailures = [];

  for (const record of Records) {
    const messageId = record.messageId;
    try {
      let eventData;
      // Parse SQS message body
      try {
        eventData = JSON.parse(record.body);
      } catch (e) {
        console.error(e);
        // Unparseable message — send straight to DLQ, don't retry
        console.error("Unparseable SQS message body — routing to DLQ", {
          messageId,
        });

        // Manually sending to DLQ without adding to batch failure
        const command = new SendMessageCommand({
          QueueUrl: DLQURL,
          MessageBody: JSON.stringify(record),
        });
        const result = await sqsClient.send(command);
        console.error("Sent message to DLQ", { messageId: result.MessageId });

        continue;
      }

      // Validate event structure and required fields
      const valid = validateEvent(eventData);
      if (!valid.valid) {
        // Send invalid events straight to DLQ
        console.error("Event validation failed", {
          errors: valid.errors,
        });

        // Manually sending to DLQ without adding to batch failure
        const command = new SendMessageCommand({
          QueueUrl: DLQURL,
          MessageBody: JSON.stringify(record),
        });

        const result = await sqsClient.send(command);
        console.error("Sent message to DLQ", { messageId: result.MessageId });

        continue;
      }

      const { device_id, type, value, ts, request_id, event_id } = valid.data;
      console.log("Processing event", {
        messageId,
        device_id,
        type,
        value,
      });

      // Fetch all enabled rules for this device and metric type
      const rulesResult = await dynamo.send(
        new QueryCommand({
          TableName: RULES_TABLE,
          IndexName: "device_id-index",
          KeyConditionExpression: "device_id = :did",
          FilterExpression: "#en = :true AND #mt = :type",
          ExpressionAttributeNames: {
            "#en": "enabled",
            "#mt": "metric",
          },
          ExpressionAttributeValues: {
            ":did": device_id,
            ":true": true,
            ":type": type,
          },
        }),
      );

      const rules = rulesResult.Items ?? [];
      console.log("rule loaded", rules);

      const triggeredAlerts = [];

      // Evaluate each rule against the current value
      for (const rule of rules) {
        const triggered = evaluate(value, rule.operator, rule.threshold);
        console.log("Rule evaluated", {
          rule_id: rule.rule_id,
          operator: rule.operator,
          threshold: rule.threshold,
          value,
          triggered,
        });

        if (triggered) {
          triggeredAlerts.push({
            rule_id: rule.rule_id,
            device_id,
            metric: rule.metric,
            operator: rule.operator,
            threshold: rule.threshold,
            actual: value,
          });
        }
      }

      // Store all triggered alerts in DynamoDB
      for (const alert of triggeredAlerts) {
        const alertId = randomUUID();
        const firedAt = Date.now();
        const alertSK = `TS#${firedAt}#${alertId}`;

        await dynamo.send(
          new PutCommand({
            TableName: ALERTS_TABLE,
            Item: {
              PK: `ALERT#${device_id}`,
              SK: alertSK,
              alert_id: alertId,
              device_id,
              rule_id: alert.rule_id,
              triggered_at: firedAt,
              event_id,
              value,
            },
          }),
        );

        console.log("warn", "ALERT TRIGGERED", {
          alert_id: alertId,
          device_id,
          metric: alert.metric,
          rule_id: alert.rule_id,
          operator: alert.operator,
          threshold: alert.threshold,
          actual: alert.actual,
        });
      }

      console.log("Message processed", {
        messageId,
        device_id,
        metric: type,
        value,
        triggeredCount: triggeredAlerts.length,
      });
    } catch (err) {
      // Unexpected error — mark for retry / eventual DLQ routing
      console.error("Unexpected error processing message — will retry", {
        messageId,
        error: err.message,
        stack: err.stack,
      });
      batchItemFailures.push({ itemIdentifier: messageId });
    }
  }

  return { batchItemFailures };
};
