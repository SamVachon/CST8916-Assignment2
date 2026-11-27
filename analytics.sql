-- AI assistance: ChatGPT helped draft these Stream Analytics queries.

SELECT
    System.Timestamp() AS windowEndUtc,
    deviceType,
    COUNT(*) AS eventCount
INTO deviceTraffic
FROM clickstream TIMESTAMP BY [timestamp]
GROUP BY TumblingWindow(second, 10), deviceType;

SELECT
    System.Timestamp() AS alertTimeUtc,
    COUNT(*) AS clickCount
INTO spikeAlerts
FROM clickstream TIMESTAMP BY [timestamp]
WHERE event_type = 'product_click'
GROUP BY SlidingWindow(second, 10)
HAVING COUNT(*) >= 3;

SELECT *
INTO rawFeed
FROM clickstream TIMESTAMP BY [timestamp];